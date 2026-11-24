//go:build functional

package offers_and_concurrent_start

import (
	"net/http"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkOfferRateOverflow(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/configs":
			tests.WriteSourceConfig(w, 4, map[string]int{}, 10)
		case "/tariff-zone/zone-1":
			tests.WriteJSON(w, map[string]any{
				"id": "zone-1", "price_per_minute": int64(1 << 61), "price_unlock": 40, "default_deposit": 250,
			})
		case "/scooter/low-charge":
			tests.WriteJSON(w, map[string]any{"id": "low-charge", "zone_id": "zone-1", "charge": 27})
		default:
			return false
		}
		return true
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL).URL
	rejected := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
		ScooterID: "full-charge", UserID: "user-1", IdempotencyID: "overflow",
	}, http.StatusServiceUnavailable)
	if rejected.ErrorCode != "SOURCE_UNAVAILABLE" {
		t.Fatalf("ответ для непредставимого тарифа: %+v", rejected)
	}
	var offerExists bool
	err := s.DB.QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM offers WHERE idempotency_id = 'overflow')`).Scan(&offerExists)
	if err != nil || offerExists {
		t.Fatalf("оффер с непредставимым тарифом сохранён: %t, %v", offerExists, err)
	}

	quote := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
		ScooterID: "low-charge", UserID: "user-1", IdempotencyID: "with-discount",
	}, http.StatusCreated)
	const wantRate = (1 << 63) * 4 / 5
	if quote.MinuteRate != wantRate || quote.UnlockFee != 40 || quote.Deposit != 250 {
		t.Fatalf("условия оффера со скидкой: %+v, ожидаемый тариф %d", quote, wantRate)
	}
}
