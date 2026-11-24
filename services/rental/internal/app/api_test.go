package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
)

func serve(t *testing.T, result error, method, path, body string) (int, map[string]any) {
	t.Helper()
	router := newAPIRouter(func(context.Context) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)),
		new(offer.Service).RegisterRoutes, func(e *echo.Echo, middleware ...echo.MiddlewareFunc) {
			e.POST("/orders/:order_id/finish", func(*echo.Context) error { return result }, middleware...)
		})
	return request(t, router, method, path, body)
}

func request(t *testing.T, router http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
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

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("%w: order is STARTING", order.ErrOrderNotActive), http.StatusConflict, "ORDER_NOT_ACTIVE"},
		{order.ErrScooterBusy, http.StatusConflict, "SCOOTER_BUSY"},
		{offer.ErrOfferExpired, http.StatusConflict, "OFFER_EXPIRED"},
		{source.ErrZoneStale, http.StatusServiceUnavailable, "ZONE_STALE"},
		{offer.ErrSourceUnavailable, http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{order.ErrSourceUnavailable, http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{source.ErrTariffUnavailable, http.StatusServiceUnavailable, "SOURCE_UNAVAILABLE"},
		{order.ErrOverloaded, http.StatusServiceUnavailable, "OVERLOADED"},
		{errors.New("database is down"), http.StatusInternalServerError, "INTERNAL_ERROR"},
	}
	for _, tt := range tests {
		status, got := serve(t, tt.err, "POST", "/orders/"+uuid.NewString()+"/finish", "")
		if status != tt.status || got["error_code"] != tt.code {
			t.Errorf("%v: %d %v", tt.err, status, got)
		}
	}
}

func TestRoutingErrors(t *testing.T) {
	if status, got := serve(t, nil, "GET", "/nope", ""); status != http.StatusNotFound || got["error_code"] != "NOT_FOUND" {
		t.Fatalf("unknown route %d %v", status, got)
	}
	if status, got := serve(t, nil, "DELETE", "/offers", ""); status != http.StatusMethodNotAllowed || got["error_code"] != "METHOD_NOT_ALLOWED" {
		t.Fatalf("wrong method %d %v", status, got)
	}
}

func TestInvalidStartRequests(t *testing.T) {
	router := newAPIRouter(func(context.Context) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)),
		new(order.Service).RegisterRoutes)
	if status, got := request(t, router, "POST", "/orders", `{"offer_id":"nope"}`); status != http.StatusNotFound || got["error_code"] != "NOT_FOUND" {
		t.Fatalf("malformed id %d %v", status, got)
	}
	if status, got := request(t, router, "POST", "/orders", `{`); status != http.StatusBadRequest || got["error_code"] != "INVALID_REQUEST" {
		t.Fatalf("bad JSON %d %v", status, got)
	}
}
