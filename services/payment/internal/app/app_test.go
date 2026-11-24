package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/payment/internal/ledger"
)

func call(t *testing.T, router http.Handler, method, path, body string) (int, map[string]string) {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("%s %s: %q: %v", method, path, rec.Body.String(), err)
	}
	return rec.Code, decoded
}

func TestContract(t *testing.T) {
	router := NewRouter(ledger.New(func() time.Time { return time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC) }, false))
	hold := `{"operation_id":"op","user_id":"u","amount":300,"hold_expires_at":"2026-11-24T13:00:00Z"}`

	steps := []struct {
		method, path, body string
		status             int
		key, want          string
	}{
		{"GET", "/payment/op", "", http.StatusNotFound, "error_code", "NOT_FOUND"},
		{"POST", "/payments/hold", hold, http.StatusOK, "status", "success"},
		{"GET", "/payment/op", "", http.StatusOK, "status", "HELD"},
		{"POST", "/payments/hold", strings.Replace(hold, "300", "1", 1), http.StatusConflict, "error_code", "PARAMETER_CONFLICT"},
		{"POST", "/payments/settle", `{"operation_id":"op","user_id":"x","total_amount":1}`, http.StatusConflict, "error_code", "USER_MISMATCH"},
		{"POST", "/payments/settle", `{"operation_id":"op","user_id":"u","total_amount":120}`, http.StatusOK, "status", "success"},
		{"GET", "/payment/op", "", http.StatusOK, "status", "PAID"},
		{"POST", "/payments/void", `{"operation_id":"op"}`, http.StatusConflict, "error_code", "INVALID_ORDER"},
		{"POST", "/payments/void", `{"operation_id":"other"}`, http.StatusOK, "status", "success"},
		{"POST", "/payments/hold", `{`, http.StatusBadRequest, "error_code", "INVALID_REQUEST"},
	}
	for _, s := range steps {
		status, body := call(t, router, s.method, s.path, s.body)
		if status != s.status || body[s.key] != s.want {
			t.Fatalf("%s %s %s: %d %v", s.method, s.path, s.body, status, body)
		}
	}
}

func TestSettleRequiresNonNegativeIntegerAmount(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	l := ledger.New(func() time.Time { return now }, false)
	if _, err := l.Hold("op", "u", 300, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	router := NewRouter(l)
	const fields = `{"operation_id":"op","user_id":"u"`

	for _, tt := range []struct {
		name        string
		amountField string
	}{
		{name: "missing"},
		{name: "null", amountField: `,"total_amount":null`},
		{name: "fraction", amountField: `,"total_amount":1.5`},
		{name: "exponent", amountField: `,"total_amount":1e2`},
		{name: "string", amountField: `,"total_amount":"10"`},
		{name: "negative", amountField: `,"total_amount":-1`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := call(t, router, http.MethodPost, "/payments/settle", fields+tt.amountField+"}")
			if status != http.StatusBadRequest || body["error_code"] != "INVALID_REQUEST" {
				t.Fatalf("SETTLE = %d %v, want 400 INVALID_REQUEST", status, body)
			}
			status, body = call(t, router, http.MethodGet, "/payment/op", "")
			if status != http.StatusOK || body["status"] != ledger.StatusHeld {
				t.Fatalf("payment = %d %v, want 200 HELD", status, body)
			}
		})
	}

	status, body := call(t, router, http.MethodPost, "/payments/settle", fields+`,"total_amount":0}`)
	if status != http.StatusOK || body["status"] != ledger.Success {
		t.Fatalf("SETTLE with explicit zero = %d %v, want 200 success", status, body)
	}
	status, body = call(t, router, http.MethodGet, "/payment/op", "")
	if status != http.StatusOK || body["status"] != ledger.StatusPaid {
		t.Fatalf("payment after zero SETTLE = %d %v, want 200 PAID", status, body)
	}
}

func TestSettleLargeAmountsAreExact(t *testing.T) {
	router := NewRouter(ledger.New(time.Now, false))
	const fields = `{"operation_id":"op","user_id":"u","total_amount":`
	for range 2 {
		status, body := call(t, router, http.MethodPost, "/payments/settle", fields+"9223372036854775847}")
		if status != http.StatusOK || body["status"] != ledger.Success {
			t.Fatalf("exact SETTLE = %d %v, want 200 success", status, body)
		}
	}

	status, body := call(t, router, http.MethodPost, "/payments/settle", fields+"9223372036854775848}")
	if status != http.StatusConflict || body["error_code"] != "PARAMETER_CONFLICT" {
		t.Fatalf("SETTLE with a different amount = %d %v, want 409 PARAMETER_CONFLICT", status, body)
	}
}
