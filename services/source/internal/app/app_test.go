package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/catalog"
)

func get(t *testing.T, router http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func TestContract(t *testing.T) {
	service, err := catalog.NewService(100, 100)
	if err != nil {
		t.Fatal(err)
	}
	router := newRouter(service)

	status, scooter := get(t, router, "/scooter/scooter-1")
	if status != http.StatusOK || scooter["id"] != "scooter-1" || scooter["zone_id"] != "zone-2" || scooter["charge"] != 37.0 {
		t.Fatalf("scooter %d %v", status, scooter)
	}
	status, zone := get(t, router, "/tariff-zone/zone-2")
	if status != http.StatusOK || zone["id"] != "zone-2" || zone["price_per_minute"] != 12.0 ||
		zone["price_unlock"] != 40.0 || zone["default_deposit"] != 250.0 {
		t.Fatalf("zone %d %v", status, zone)
	}
	status, user := get(t, router, "/user-profile/user-12")
	if status != http.StatusOK || user["id"] != "user-12" || user["has_subscription"] != true || user["trusted"] != true {
		t.Fatalf("user %d %v", status, user)
	}
	status, configs := get(t, router, "/configs")
	coeffs, _ := configs["price_coeff_settings"].(map[string]any)
	retention, hasRetention := configs["retention_days_by_zone"].(map[string]any)
	if status != http.StatusOK || coeffs["surge"] != 1.25 || coeffs["low_charge_discount"] != 0.8 ||
		!hasRetention || len(retention) != 0 || configs["default_retention_days"] != 365.0 || configs["tariff_zone_cache_ttl_minutes"] != 10.0 {
		t.Fatalf("configs %d %v", status, configs)
	}
	for _, path := range []string{"/scooter/scooter-101", "/tariff-zone/zone-9", "/user-profile/nobody"} {
		if status, body := get(t, router, path); status != http.StatusNotFound || body["error_code"] != "NOT_FOUND" {
			t.Fatalf("%s: %d %v", path, status, body)
		}
	}
}
