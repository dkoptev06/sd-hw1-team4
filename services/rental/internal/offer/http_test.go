package offer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

func TestCreateOfferHTTP(t *testing.T) {
	f := newOfferFixture()
	router := echo.New()
	f.service.RegisterRoutes(router)
	send := func() (int, offerResponse) {
		t.Helper()
		rec := httptest.NewRecorder()
		body := strings.NewReader(`{"scooter_id":"s","user_id":"u","idempotency_id":"k"}`)
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/offers", body))
		var response offerResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return rec.Code, response
	}
	code, created := send()
	stored := f.store.byKey["k"]
	if code != http.StatusCreated || created.OfferID != stored.id.String() || created.MinuteRate != 9 ||
		created.UnlockFee != 50 || created.Deposit != 300 || !created.ExpiresAt.Equal(stored.expiresAt) ||
		created.ExpiresAt.Location() != time.UTC {
		t.Fatalf("create %d %+v", code, created)
	}
	if stored.scooter.ID != "s" || stored.request.userID != "u" {
		t.Fatalf("stored request %+v", stored.request)
	}
	if code, replay := send(); code != http.StatusOK || replay != created {
		t.Fatalf("replay %d %+v", code, replay)
	}
}
