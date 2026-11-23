package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/rental"
)

type fakeOffers struct {
	offer   domain.Offer
	created bool
	err     error
	got     rental.CreateOfferRequest
}

func (f *fakeOffers) Create(_ context.Context, req rental.CreateOfferRequest) (domain.Offer, bool, error) {
	f.got = req
	return f.offer, f.created, f.err
}

type fakeOrders struct {
	start  rental.StartResult
	view   rental.OrderView
	finish domain.Order
	err    error
}

func (f *fakeOrders) Start(context.Context, uuid.UUID) (rental.StartResult, error) {
	return f.start, f.err
}
func (f *fakeOrders) Get(context.Context, uuid.UUID) (rental.OrderView, error) { return f.view, f.err }
func (f *fakeOrders) Finish(context.Context, uuid.UUID) (domain.Order, error)  { return f.finish, f.err }

func serve(t *testing.T, offers Offers, orders Orders, method, path, body string) (int, map[string]any) {
	t.Helper()
	router := NewRouter(NewAPI(offers, orders), func(context.Context) error { return nil },
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("body %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, decoded
}

func TestCreateOffer(t *testing.T) {
	expires := time.Date(2026, 11, 24, 12, 2, 0, 0, time.UTC)
	offers := &fakeOffers{offer: domain.Offer{ID: uuid.New(), Terms: domain.Terms{MinuteRate: 9, UnlockFee: 50, Deposit: 300}, ExpiresAt: expires}, created: true}
	status, body := serve(t, offers, &fakeOrders{}, "POST", "/offers", `{"scooter_id":"s","user_id":"u","idempotency_id":"k"}`)
	if status != http.StatusCreated || body["offer_id"] != offers.offer.ID.String() || body["minute_rate"] != 9.0 ||
		body["deposit"] != 300.0 || body["expires_at"] != "2026-11-24T12:02:00Z" {
		t.Fatalf("%d %v", status, body)
	}
	if offers.got != (rental.CreateOfferRequest{ScooterID: "s", UserID: "u", IdempotencyID: "k"}) {
		t.Fatalf("request %+v", offers.got)
	}
	offers.created = false
	if status, _ := serve(t, offers, &fakeOrders{}, "POST", "/offers", `{}`); status != http.StatusOK {
		t.Fatalf("replay status %d", status)
	}
}

func TestStartOrder(t *testing.T) {
	orders := &fakeOrders{start: rental.StartResult{OrderID: uuid.New(), Status: domain.OrderStarting}}
	path, body := "/orders", `{"offer_id":"`+uuid.NewString()+`"}`
	if status, got := serve(t, &fakeOffers{}, orders, "POST", path, body); status != http.StatusAccepted || got["status"] != "STARTING" {
		t.Fatalf("%d %v", status, got)
	}
	orders.start.Replayed, orders.start.Status = true, domain.OrderActive
	if status, got := serve(t, &fakeOffers{}, orders, "POST", path, body); status != http.StatusOK || got["status"] != "ACTIVE" {
		t.Fatalf("replay %d %v", status, got)
	}
	if status, got := serve(t, &fakeOffers{}, orders, "POST", path, `{"offer_id":"nope"}`); status != http.StatusNotFound || got["error_code"] != "NOT_FOUND" {
		t.Fatalf("malformed id %d %v", status, got)
	}
	if status, got := serve(t, &fakeOffers{}, orders, "POST", path, `{`); status != http.StatusBadRequest || got["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("bad JSON %d %v", status, got)
	}
}

func TestGetOrderNullFields(t *testing.T) {
	orders := &fakeOrders{view: rental.OrderView{Order: domain.Order{ID: uuid.New(), Status: domain.OrderStarting}}}
	status, got := serve(t, &fakeOffers{}, orders, "GET", "/orders/"+uuid.NewString(), "")
	if status != http.StatusOK || got["status"] != "STARTING" {
		t.Fatalf("%d %v", status, got)
	}
	for _, field := range []string{"cost", "started_at", "finish_time"} {
		if value, present := got[field]; !present || value != nil {
			t.Fatalf("%s must be null, got %v (present=%t)", field, value, present)
		}
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{domain.Wrap(domain.ErrOrderNotActive, "order is STARTING"), http.StatusConflict, "ORDER_NOT_ACTIVE"},
		{domain.ErrScooterBusy, http.StatusConflict, "SCOOTER_BUSY"},
		{domain.ErrOfferExpired, http.StatusConflict, "OFFER_EXPIRED"},
		{domain.ErrZoneStale, http.StatusServiceUnavailable, "ZONE_STALE"},
		{domain.ErrSourceUnavailable, http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{errors.New("database is down"), http.StatusInternalServerError, "INTERNAL_ERROR"},
	}
	for _, tt := range tests {
		orders := &fakeOrders{err: tt.err}
		status, got := serve(t, &fakeOffers{}, orders, "POST", "/orders/"+uuid.NewString()+"/finish", "")
		if status != tt.status || got["error_code"] != tt.code {
			t.Errorf("%v: %d %v", tt.err, status, got)
		}
	}
}

func TestRoutingErrors(t *testing.T) {
	if status, got := serve(t, &fakeOffers{}, &fakeOrders{}, "GET", "/nope", ""); status != http.StatusNotFound || got["error_code"] != "NOT_FOUND" {
		t.Fatalf("unknown route %d %v", status, got)
	}
	if status, got := serve(t, &fakeOffers{}, &fakeOrders{}, "DELETE", "/offers", ""); status != http.StatusMethodNotAllowed || got["error_code"] != "METHOD_NOT_ALLOWED" {
		t.Fatalf("wrong method %d %v", status, got)
	}
}
