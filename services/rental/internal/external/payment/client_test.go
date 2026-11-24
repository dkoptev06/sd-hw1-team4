package payment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func TestClassification(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    task.Outcome
	}{
		{"success", reply(http.StatusOK, `{"status":"success"}`), task.Success},
		{"failed", reply(http.StatusOK, `{"status":"failed"}`), task.Rejected},
		{"conflict", reply(http.StatusConflict, `{"error_code":"PARAMETER_CONFLICT"}`), task.ContractError},
		{"bad request", reply(http.StatusBadRequest, `{}`), task.ContractError},
		{"5xx", reply(http.StatusServiceUnavailable, ``), task.Transient},
		{"unknown status", reply(http.StatusOK, `{"status":"maybe"}`), task.Transient},
		{"broken body", reply(http.StatusOK, `{`), task.Transient},
		{"timeout", func(w http.ResponseWriter, r *http.Request) { time.Sleep(300 * time.Millisecond) }, task.Transient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			client := NewClient(server.URL, httpx.NewClient(), 100*time.Millisecond)
			if got := client.Void(context.Background(), task.VoidParams{OperationID: "op"}); got.Outcome != tt.want {
				t.Fatalf("outcome = %s (%s), want %s", got.Outcome, got.Detail, tt.want)
			}
		})
	}
}

func TestIncompleteResponseBody(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		want       task.Outcome
	}{
		{name: "4xx stays a contract error", statusCode: http.StatusConflict, want: task.ContractError},
		{name: "200 stays indeterminate", statusCode: http.StatusOK, want: task.Transient},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "64")
				w.WriteHeader(test.statusCode)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "success"})
			}))
			defer server.Close()
			client := NewClient(server.URL, httpx.NewClient(), time.Second)
			got := client.Void(t.Context(), task.VoidParams{OperationID: "op"})
			if got.Outcome != test.want {
				t.Fatalf("outcome = %s (%s), want %s", got.Outcome, got.Detail, test.want)
			}
			if test.want == task.ContractError && !strings.HasPrefix(got.Detail, "status 409:") {
				t.Fatalf("response status missing from error: %s", got.Detail)
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
	client := NewClient(server.URL, httpx.NewClient(), time.Second)
	result := client.Hold(context.Background(), task.HoldParams{OperationID: "op", UserID: "u", Amount: 300, HoldExpiresAt: expires})
	if result.Outcome != task.Success {
		t.Fatalf("hold = %s %s", result.Outcome, result.Detail)
	}
}

func reply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func TestContractErrorBodyIsStorableText(t *testing.T) {
	for _, test := range []struct{ name, body, want string }{
		{name: "localized", body: "ошибка", want: "ошибка"},
		{name: "truncated rune", body: strings.Repeat("a", 199) + "я", want: strings.Repeat("a", 199) + "?"},
		{name: "zero byte", body: "a\x00b", want: "a?b"},
		{name: "invalid UTF-8", body: "a\xffb", want: "a?b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := classify(httpx.Response{Status: http.StatusConflict, Body: []byte(test.body)}, nil)
			if got.Outcome != task.ContractError || got.Detail != "status 409: "+test.want {
				t.Fatalf("unexpected Payment result: %+v", got)
			}
		})
	}
}
