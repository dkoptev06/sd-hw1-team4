//go:build functional

package offers_and_concurrent_start

import (
	"net/http"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkExpiredOffer(t *testing.T, s *tests.Stand, client *http.Client, api string) {
	quote := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers",
		tests.OfferRequest{ScooterID: "expired", UserID: "user-1", IdempotencyID: "expired"}, http.StatusCreated)
	if _, err := s.DB.Exec(t.Context(), `UPDATE offers SET expires_at = clock_timestamp() WHERE offer_id = $1`, quote.OfferID); err != nil {
		t.Fatal(err)
	}
	rejected := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: quote.OfferID}, http.StatusConflict)
	if rejected.ErrorCode != "OFFER_EXPIRED" {
		t.Fatalf("неожиданный код для истёкшего оффера: %q", rejected.ErrorCode)
	}
	var orders int
	err := s.DB.QueryRow(t.Context(), `SELECT count(*) FROM orders WHERE offer_id = $1`, quote.OfferID).Scan(&orders)
	if err != nil || orders != 0 {
		t.Fatalf("найдено заказов по истёкшему офферу: %d, ошибка: %v", orders, err)
	}
}
