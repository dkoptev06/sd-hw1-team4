//go:build functional

package source_failures_and_caches

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkScooterFreshness(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	type scooterState struct {
		zone   string
		charge int
		failed bool
	}
	var current atomic.Pointer[scooterState]
	initial := &scooterState{zone: "zone-1", charge: 30}
	current.Store(initial)
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		scooter, ok := strings.CutPrefix(r.URL.Path, "/scooter/")
		if !ok {
			return false
		}
		state := current.Load()
		if state.failed {
			http.Error(w, "scooters unavailable", http.StatusServiceUnavailable)
			return true
		}
		tests.WriteJSON(w, map[string]any{"id": scooter, "zone_id": state.zone, "charge": state.charge})
		return true
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL).URL

	current.Store(&scooterState{failed: true})
	got := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
		ScooterID: "down", UserID: "trusted-user", IdempotencyID: "down",
	}, http.StatusServiceUnavailable)
	if got.ErrorCode != "SOURCE_UNAVAILABLE" {
		t.Fatalf("ошибка создания оффера: %+v", got)
	}
	var activeOrderID string
	for _, test := range []struct {
		key     string
		current scooterState
		status  int
		code    string
	}{
		{key: "недоступность", current: scooterState{failed: true}, status: http.StatusServiceUnavailable, code: "SOURCE_UNAVAILABLE"},
		{key: "другая зона", current: scooterState{zone: "zone-2", charge: 30}, status: http.StatusConflict, code: "OFFER_CHANGED"},
		{key: "ниже порога", current: scooterState{zone: "zone-1", charge: 27}, status: http.StatusConflict, code: "OFFER_CHANGED"},
		{key: "на пороге", current: scooterState{zone: "zone-1", charge: 28}, status: http.StatusAccepted},
	} {
		current.Store(initial)
		offer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: test.key, UserID: "trusted-user", IdempotencyID: test.key,
		}, http.StatusCreated)
		current.Store(&test.current)
		start := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: offer.OfferID}, test.status)
		if start.ErrorCode != test.code || (test.code == "" && (start.OrderID == "" || start.Status != "STARTING")) {
			t.Fatalf("старт при изменении %q: %+v", test.key, start)
		}
		if test.status == http.StatusAccepted {
			activeOrderID = start.OrderID
		}
	}
	var offers, orders int
	err := s.DB.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM offers), (SELECT count(*) FROM orders)`).Scan(&offers, &orders)
	if err != nil || offers != 4 || orders != 1 {
		t.Fatalf("после ошибок сохранились лишние записи: офферов %d, заказов %d, %v", offers, orders, err)
	}

	payment := tests.NewPaymentStub(t)
	s.StartRental(t, "worker", "SOURCE_URL="+source.URL, "PAYMENT_URL="+payment.URL, "WORKER_POLL_INTERVAL=20ms")
	tests.WaitForOrder(t, client, api, activeOrderID, "ACTIVE")

	var startedAt time.Time
	err = s.DB.QueryRow(t.Context(), `
		UPDATE orders SET started_at = clock_timestamp() - interval '61 seconds'
		WHERE order_id = $1 RETURNING started_at`, activeOrderID).Scan(&startedAt)
	if err != nil {
		t.Fatal(err)
	}
	current.Store(&scooterState{failed: true})
	view := tests.WaitForOrder(t, client, api, activeOrderID, "ACTIVE")
	if view.Cost == nil || *view.Cost < 55 || view.FinishTime != nil || view.StartedAt == nil || !view.StartedAt.Equal(startedAt) {
		t.Fatalf("активный заказ при недоступном scooters: %+v", view)
	}
	finished := tests.RequirePost[tests.FinishResponse](t, client, api+"/orders/"+activeOrderID+"/finish", nil, http.StatusOK)
	if finished.OrderID != activeOrderID || finished.Status != "COMPLETED" || finished.FinishTime == nil || finished.Cost == nil {
		t.Fatalf("завершение поездки при недоступном scooters: %+v", finished)
	}
	wantCost := int64(40) + int64(finished.FinishTime.Sub(startedAt)/time.Second)*15/60
	if *finished.Cost != wantCost {
		t.Fatalf("стоимость завершённой поездки %d, ожидалась %d", *finished.Cost, wantCost)
	}
	s.WaitPaymentTask(t, activeOrderID, "SETTLE", "COMPLETED")
}
