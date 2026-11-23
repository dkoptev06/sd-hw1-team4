package payment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpjson"
)

func TestClassification(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    Outcome
	}{
		{"success", reply(http.StatusOK, `{"status":"success"}`), Success},
		{"failed", reply(http.StatusOK, `{"status":"failed"}`), Rejected},
		{"conflict", reply(http.StatusConflict, `{"error_code":"PARAMETER_CONFLICT"}`), ContractError},
		{"bad request", reply(http.StatusBadRequest, `{}`), ContractError},
		{"5xx", reply(http.StatusServiceUnavailable, ``), Transient},
		{"unknown status", reply(http.StatusOK, `{"status":"maybe"}`), Transient},
		{"broken body", reply(http.StatusOK, `{`), Transient},
		{"timeout", func(w http.ResponseWriter, r *http.Request) { time.Sleep(300 * time.Millisecond) }, Transient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			client := NewClient(server.URL, httpjson.NewClient(), 100*time.Millisecond)
			if got := client.Void(context.Background(), domain.VoidParams{OperationID: "op"}); got.Outcome != tt.want {
				t.Fatalf("outcome = %s (%s), want %s", got.Outcome, got.Detail, tt.want)
			}
		})
	}
}

func TestHoldRequest(t *testing.T) {
	expires := time.Date(2026, 11, 24, 13, 2, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.URL.Path != "/payments/hold" || json.NewDecoder(r.Body).Decode(&body) != nil ||
			body["operation_id"] != "op" || body["user_id"] != "u" || body["amount"] != 300.0 ||
			body["hold_expires_at"] != "2026-11-24T13:02:00Z" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()
	client := NewClient(server.URL, httpjson.NewClient(), time.Second)
	result := client.Hold(context.Background(), domain.HoldParams{OperationID: "op", UserID: "u", Amount: 300, HoldExpiresAt: expires})
	if result.Outcome != Success {
		t.Fatalf("hold = %s %s", result.Outcome, result.Detail)
	}
}

func reply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}
