//go:build functional

package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
)

type PaymentCall struct {
	Method      string
	OperationID string
}

type PaymentStub struct {
	URL   string
	mu    sync.Mutex
	calls []PaymentCall
}

func NewPaymentStub(t *testing.T) *PaymentStub {
	t.Helper()
	payment := &PaymentStub{}
	mux := http.NewServeMux()
	for _, method := range []string{"hold", "void", "settle"} {
		mux.HandleFunc("POST /payments/"+method, func(w http.ResponseWriter, r *http.Request) {
			var params struct {
				OperationID string `json:"operation_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil || params.OperationID == "" {
				http.Error(w, "invalid payment request", http.StatusBadRequest)
				return
			}
			payment.mu.Lock()
			payment.calls = append(payment.calls, PaymentCall{Method: method, OperationID: params.OperationID})
			payment.mu.Unlock()
			WriteJSON(w, map[string]string{"status": "success"})
		})
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	payment.URL = server.URL
	return payment
}

func (p *PaymentStub) RecordedCalls() []PaymentCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}
