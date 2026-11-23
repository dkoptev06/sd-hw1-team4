package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestContractRoutes(t *testing.T) {
	router := newRouter()
	routes := []struct{ method, path string }{
		{"GET", "/scooter/scooter-id"},
		{"GET", "/tariff-zone/zone-id"},
		{"GET", "/user-profile/user-id"},
		{"GET", "/configs"},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			request := httptest.NewRequest(route.method, route.path, nil)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501", response.Code)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q", got)
			}
			var body struct {
				ErrorCode string `json:"error_code"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.ErrorCode != "NOT_IMPLEMENTED" {
				t.Fatalf("unexpected error body: %s (%v)", response.Body.String(), err)
			}
		})
	}
}
