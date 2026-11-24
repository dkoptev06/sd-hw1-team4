//go:build functional

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type PaymentGate struct {
	reached chan struct{}
	release chan struct{}
}

func NewPaymentGate() *PaymentGate {
	return &PaymentGate{reached: make(chan struct{}), release: make(chan struct{})}
}

func (g *PaymentGate) block(ctx context.Context) bool {
	close(g.reached)
	select {
	case <-g.release:
		return true
	case <-ctx.Done():
		return false
	}
}

type PaymentFaults struct {
	BeforeHold     *PaymentGate
	AfterHold      *PaymentGate
	DropHold       bool
	DropVoid       bool
	DropSettle     bool
	ResponseStatus func(path string) int
}

type PaymentAttempt struct {
	Path    string
	Body    string
	Outcome string
}

type PaymentProxy struct {
	URL      string
	forward  http.Handler
	faults   PaymentFaults
	mu       sync.Mutex
	attempts []PaymentAttempt
	dropped  []string
}

func NewPaymentProxy(t *testing.T, upstream string, faults PaymentFaults) *PaymentProxy {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	proxy := &PaymentProxy{forward: httputil.NewSingleHostReverseProxy(target), faults: faults}
	server := httptest.NewServer(http.HandlerFunc(proxy.serveHTTP))
	t.Cleanup(server.Close)
	proxy.URL = server.URL
	return proxy
}

func (p *PaymentProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read payment request", http.StatusBadRequest)
		return
	}
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	p.mu.Lock()
	first := !slices.ContainsFunc(p.attempts, func(a PaymentAttempt) bool { return a.Path == r.URL.Path })
	index := len(p.attempts)
	p.attempts = append(p.attempts, PaymentAttempt{Path: r.URL.Path, Body: string(body)})
	p.mu.Unlock()

	isHold, isVoid := r.URL.Path == "/payments/hold", r.URL.Path == "/payments/void"
	if first && isHold && p.faults.BeforeHold != nil && !p.faults.BeforeHold.block(r.Context()) {
		return
	}
	if p.faults.ResponseStatus != nil {
		if status := p.faults.ResponseStatus(r.URL.Path); status != 0 {
			http.Error(w, http.StatusText(status), status)
			return
		}
	}

	// Payment performs the command before the proxy decides whether Rental sees its response.
	reply := httptest.NewRecorder()
	p.forward.ServeHTTP(reply, r)
	var result struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(reply.Body.Bytes(), &result) == nil {
		p.mu.Lock()
		p.attempts[index].Outcome = result.Status
		p.mu.Unlock()
	}
	if first && isHold && p.faults.AfterHold != nil && !p.faults.AfterHold.block(r.Context()) {
		return
	}
	dropResponse := isHold && p.faults.DropHold || isVoid && p.faults.DropVoid ||
		r.URL.Path == "/payments/settle" && p.faults.DropSettle
	if first && dropResponse {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			p.mu.Lock()
			p.dropped = append(p.dropped, r.URL.Path)
			p.mu.Unlock()
			conn.Close()
			return
		}
		http.Error(w, "could not drop connection", http.StatusBadGateway)
		return
	}
	for name, values := range reply.Header() {
		w.Header()[name] = values
	}
	w.WriteHeader(reply.Code)
	_, _ = w.Write(reply.Body.Bytes())
}

func (p *PaymentProxy) RecordedAttempts() []PaymentAttempt {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.attempts)
}

func (p *PaymentProxy) DroppedResponses() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.dropped)
}

func RequirePaymentCommands(t *testing.T, attempts []PaymentAttempt, expected ...PaymentCall) {
	t.Helper()
	got, want := map[PaymentCall]int{}, map[PaymentCall]int{}
	for _, attempt := range attempts {
		method, ok := strings.CutPrefix(attempt.Path, "/payments/")
		var params struct {
			OperationID string `json:"operation_id"`
		}
		if err := json.Unmarshal([]byte(attempt.Body), &params); !ok || err != nil {
			t.Fatalf("неожиданный запрос Payment: %+v, %v", attempt, err)
		}
		got[PaymentCall{Method: method, OperationID: params.OperationID}]++
	}
	for _, call := range expected {
		want[call]++
	}
	if !maps.Equal(got, want) {
		t.Fatalf("вызовы Payment: %+v, ожидались %+v", got, want)
	}
	t.Logf("Вызовы Payment по методу и operation_id: %v", got)
}

func (gate *PaymentGate) Wait(t *testing.T) {
	t.Helper()
	select {
	case <-gate.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("HOLD не дошёл до точки остановки")
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func (gate *PaymentGate) Release() {
	close(gate.release)
}
