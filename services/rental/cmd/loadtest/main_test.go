package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A successful archived GET and an HTTP 200 containing a different order are
// different outcomes. Losing this distinction would turn routing mistakes into
// successful throughput in the acceptance report.
func TestHistoricalGETValidatesBodyAndCountsFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte(`{"order_id":"a","status":"COMPLETED","cost":42,"started_at":"2026-11-23T23:58:00Z","finish_time":"2026-11-24T00:00:00Z"}`))
	}))
	defer server.Close()
	now := time.Now()
	r := run{
		opts: options{url: server.URL}, client: server.Client(),
		from: now.Add(-time.Minute), to: now.Add(time.Minute),
		failures: map[string]int{},
		stats:    map[string]*route{"get_history": {}},
	}
	r.startReads(2, 2)
	r.read("/orders/a", "get_history", "a", now, nil)
	r.read("/orders/b", "get_history", "b", now, nil)
	r.drainReads()
	if r.stats["get_history"].success != 1 || r.failures["get_history/invalid_order"] != 1 {
		t.Fatalf("stats %+v failures %+v", r.stats["get_history"], r.failures)
	}
}

func TestPercentileIncludesUnsuccessfulRequests(t *testing.T) {
	s := route{success: 18}
	for i := 1; i <= 20; i++ {
		s.latencies = append(s.latencies, time.Duration(i)*time.Millisecond)
	}
	got := s.summarize(2 * time.Second)
	if got.P95MS != 19 || got.Attempts != 20 || got.RPS != 9 {
		t.Fatalf("%+v", got)
	}
}

func TestReconcileReusesOfferAndFinishesOutsideCounters(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		switch req.URL.Path {
		case "/orders":
			var body map[string]string
			_ = json.NewDecoder(req.Body).Decode(&body)
			if body["offer_id"] != "existing" {
				t.Errorf("replay body %v", body)
			}
			_, _ = w.Write([]byte(`{"order_id":"r","status":"ACTIVE"}`))
		case "/orders/r":
			_, _ = w.Write([]byte(`{"order_id":"r","status":"ACTIVE"}`))
		case "/orders/r/finish":
			_, _ = w.Write([]byte(`{"order_id":"r","status":"COMPLETED"}`))
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	r := run{opts: options{url: server.URL}, client: server.Client(), unresolved: []incomplete{{offer: "existing"}}}
	if got := r.reconcile(); got != 1 || r.finished != 0 || len(calls) != 3 {
		t.Fatalf("reconciled %d, measured %d, calls %v", got, r.finished, calls)
	}
}
