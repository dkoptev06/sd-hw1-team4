//go:build functional

package source_failures_and_caches

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkTariffExpiration(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	var failTariffs atomic.Bool
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/configs" {
			tests.WriteSourceConfig(w, 1.25, map[string]int{}, 1)
			return true
		}
		if strings.HasPrefix(r.URL.Path, "/tariff-zone/") && failTariffs.Load() {
			http.Error(w, "tariff zones unavailable", http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL).URL
	request := tests.OfferRequest{ScooterID: "scooter", UserID: "trusted-user", IdempotencyID: "prime"}
	failTariffs.Store(true)
	if got := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusServiceUnavailable); got.ErrorCode != "SOURCE_UNAVAILABLE" {
		t.Fatalf("ответ при недоступном тарифе и пустом кэше: %+v", got)
	}
	failTariffs.Store(false)
	want := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusCreated)
	failTariffs.Store(true)
	request.IdempotencyID = "fresh"
	got := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusCreated)
	if got.MinuteRate != want.MinuteRate || got.UnlockFee != want.UnlockFee || got.Deposit != want.Deposit {
		t.Fatalf("свежий кэш изменил тариф: %+v → %+v", want, got)
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	request.IdempotencyID = "stale"
	if got := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusServiceUnavailable); got.ErrorCode != "ZONE_STALE" {
		t.Fatalf("ответ для просроченного тарифа: %+v", got)
	}
	failTariffs.Store(false)
	tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusCreated)
}

func checkTariffCacheEviction(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	var mu sync.Mutex
	calls := make(map[string]int)
	rates := map[string]int{"zone-1": 12, "zone-2": 24, "zone-3": 36}
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		if zone, ok := strings.CutPrefix(r.URL.Path, "/scooter/"); ok {
			tests.WriteJSON(w, map[string]any{"id": zone, "zone_id": zone, "charge": 80})
			return true
		}
		if zone, ok := strings.CutPrefix(r.URL.Path, "/tariff-zone/"); ok {
			mu.Lock()
			calls[zone]++
			mu.Unlock()
			tests.WriteJSON(w, map[string]any{
				"id": zone, "price_per_minute": rates[zone], "price_unlock": 40, "default_deposit": 250,
			})
			return true
		}
		return false
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL, "TARIFF_CACHE_SIZE=2").URL

	// Повторное чтение zone-1 делает её новее zone-2, хотя она загружена первой.
	for i, step := range []struct {
		zone  string
		rate  int64
		calls int
	}{
		{zone: "zone-1", rate: 15, calls: 1},
		{zone: "zone-2", rate: 30, calls: 1},
		{zone: "zone-1", rate: 15, calls: 1},
		{zone: "zone-3", rate: 45, calls: 1},
		{zone: "zone-1", rate: 15, calls: 1},
		{zone: "zone-2", rate: 30, calls: 2},
	} {
		offer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: step.zone, UserID: "user-1", IdempotencyID: fmt.Sprintf("lru-%d", i),
		}, http.StatusCreated)
		mu.Lock()
		gotCalls := calls[step.zone]
		mu.Unlock()
		if offer.MinuteRate != step.rate || offer.UnlockFee != 40 || offer.Deposit != 250 || gotCalls != step.calls {
			t.Fatalf("шаг %d, %s: оффер %+v, запросов к Source %d, ожидался тариф %d и %d запросов",
				i+1, step.zone, offer, gotCalls, step.rate, step.calls)
		}
	}
}
