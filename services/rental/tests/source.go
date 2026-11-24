//go:build functional

package tests

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type SourceStub struct {
	URL     string
	updated atomic.Bool
}

func NewSourceStub(t *testing.T) *SourceStub {
	t.Helper()
	return NewSourceStubWithOverride(t, nil)
}

func NewSourceStubWithOverride(t *testing.T, override func(http.ResponseWriter, *http.Request) bool) *SourceStub {
	t.Helper()
	source := &SourceStub{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /scooter/{id}", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, map[string]any{"id": r.PathValue("id"), "zone_id": "zone-1", "charge": 80})
	})
	mux.HandleFunc("GET /tariff-zone/{id}", func(w http.ResponseWriter, r *http.Request) {
		rate := 12
		if source.updated.Load() {
			rate = 28
		}
		WriteJSON(w, map[string]any{"id": r.PathValue("id"), "price_per_minute": rate, "price_unlock": 40, "default_deposit": 250})
	})
	mux.HandleFunc("GET /user-profile/{id}", func(w http.ResponseWriter, r *http.Request) {
		changed := source.updated.Load()
		WriteJSON(w, map[string]any{"id": r.PathValue("id"), "has_subscription": changed, "trusted": changed || r.PathValue("id") == "trusted-user"})
	})
	mux.HandleFunc("GET /configs", func(w http.ResponseWriter, _ *http.Request) {
		WriteSourceConfig(w, 1.25, map[string]int{}, 10)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if override != nil && override(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	source.URL = server.URL
	return source
}

func WriteSourceConfig(w http.ResponseWriter, surge float64, zoneDays map[string]int, ttlMinutes int) {
	WriteJSON(w, map[string]any{
		"price_coeff_settings":   map[string]float64{"surge": surge, "low_charge_discount": 0.8},
		"default_retention_days": 365, "retention_days_by_zone": zoneDays,
		"tariff_zone_cache_ttl_minutes": ttlMinutes,
	})
}

func (s *SourceStub) SetUpdated(updated bool) {
	s.updated.Store(updated)
}
