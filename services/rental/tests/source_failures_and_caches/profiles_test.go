//go:build functional

package source_failures_and_caches

import (
	"net/http"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkProfileFallback(t *testing.T, binary string, client *http.Client) {
	s := tests.NewStand(t, binary)
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/user-profile/timeout":
			<-r.Context().Done()
		case "/user-profile/unavailable":
			http.Error(w, "users unavailable", http.StatusBadGateway)
		case "/user-profile/bad-request":
			http.Error(w, "invalid profile request", http.StatusBadRequest)
		case "/user-profile/bad-request-incomplete":
			w.Header().Set("Content-Length", "64")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid"))
		case "/user-profile/invalid-profile":
			tests.WriteJSON(w, map[string]string{"id": "invalid-profile"})
		case "/user-profile/missing":
			http.NotFound(w, r)
		case "/user-profile/missing-incomplete":
			w.Header().Set("Content-Length", "64")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("missing"))
		default:
			return false
		}
		return true
	})
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL, "SOURCE_TIMEOUT=250ms").URL
	for _, user := range []string{"timeout", "unavailable"} {
		offer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: "scooter", UserID: user, IdempotencyID: user,
		}, http.StatusCreated)
		var fallback bool
		err := s.DB.QueryRow(t.Context(), `SELECT user_profile_fallback_used FROM offers WHERE offer_id = $1`,
			offer.OfferID).Scan(&fallback)
		if err != nil || !fallback || offer.UnlockFee != 40 || offer.Deposit != 250 {
			t.Fatalf("резервный профиль %s: %+v, fallback=%t, %v", user, offer, fallback, err)
		}
	}
	for _, tc := range []struct {
		user   string
		status int
		code   string
	}{
		{"missing", http.StatusNotFound, "NOT_FOUND"},
		{"missing-incomplete", http.StatusNotFound, "NOT_FOUND"},
		{"bad-request", http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{"bad-request-incomplete", http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{"invalid-profile", http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
	} {
		response := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: "scooter", UserID: tc.user, IdempotencyID: tc.user,
		}, tc.status)
		var exists bool
		err := s.DB.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM offers WHERE idempotency_id = $1)`, tc.user).Scan(&exists)
		if err != nil || exists || response.ErrorCode != tc.code {
			t.Fatalf("ошибка профиля %s: ответ %+v, ожидался %s, оффер=%t, %v", tc.user, response, tc.code, exists, err)
		}
	}
	tests.WaitForMetric(t, client, api, "rental_user_profile_fallbacks_total 2")
	tests.WaitForMetric(t, client, api, `rental_dependency_calls_total{dependency="source",operation="user_profile",outcome="not_found"} 2`)
}
