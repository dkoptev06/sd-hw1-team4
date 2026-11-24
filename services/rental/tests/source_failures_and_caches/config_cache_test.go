//go:build functional

package source_failures_and_caches

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkConfigRefresh(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	var configMode atomic.Int32
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/configs" {
			return false
		}
		switch configMode.Load() {
		case 0:
			http.Error(w, "configs unavailable", http.StatusServiceUnavailable)
		case 1:
			tests.WriteSourceConfig(w, 1.25, map[string]int{}, 10)
		case 2:
			tests.WriteSourceConfig(w, 1.5, map[string]int{}, 10)
		}
		return true
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL, "CONFIG_REFRESH_INTERVAL=100ms").URL
	createOffer := func(key string, status int, rate int64) tests.OfferResponse {
		t.Helper()
		offer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: "scooter", UserID: "trusted-user", IdempotencyID: key,
		}, status)
		if offer.MinuteRate != rate {
			t.Fatalf("тариф %s: получен %d, ожидался %d", key, offer.MinuteRate, rate)
		}
		return offer
	}

	tests.WaitForMetric(t, client, api, "rental_config_using_defaults 1")
	createOffer("cold", http.StatusCreated, 15)
	configMode.Store(1)
	tests.WaitForMetric(t, client, api, "rental_config_using_defaults 0")
	old := createOffer("before-update", http.StatusCreated, 15)

	configMode.Store(2)
	attempt := 0
	tests.WaitFor(t, "обновления surge до 1.5", func() (bool, error) {
		attempt++
		status, offer, err := tests.RequestJSON[tests.OfferResponse](client, http.MethodPost, api+"/offers", tests.OfferRequest{
			ScooterID: "scooter", UserID: "trusted-user", IdempotencyID: fmt.Sprintf("refresh-%d", attempt),
		})
		if err != nil || status != http.StatusCreated {
			return false, fmt.Errorf("при обновлении config: HTTP %d, %+v, %v", status, offer, err)
		}
		return offer.MinuteRate == 18, nil
	})
	const refreshFailure = `rental_config_refreshes_total{result="unavailable"}`
	unavailable, err := tests.MetricValue(client, api, refreshFailure)
	if err != nil {
		t.Fatal(err)
	}
	configMode.Store(0)
	tests.WaitFor(t, "сбоя обновления config после принятия 1.5", func() (bool, error) {
		count, err := tests.MetricValue(client, api, refreshFailure)
		return count > unavailable, err
	})
	createOffer("retained", http.StatusCreated, 18)
	if replay := createOffer("before-update", http.StatusOK, 15); replay != old {
		t.Fatalf("обновление config изменило выданный оффер: %+v → %+v", old, replay)
	}
}
