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

func TestReconcileWaitsForOfferExpiryBeforeReplay(t *testing.T) {
	expiry := time.Now().Add(150 * time.Millisecond)
	var replayedAt time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/orders" {
			http.NotFound(w, req)
			return
		}
		replayedAt = time.Now()
		if replayedAt.Before(expiry) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"order_id":"new","status":"STARTING"}`))
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":"OFFER_EXPIRED"}`))
	}))
	defer server.Close()
	r := run{
		opts:       options{url: server.URL},
		client:     server.Client(),
		unresolved: []incomplete{{offer: "unconsumed", offerExpires: &expiry}},
	}
	if got := r.reconcile(); got != 0 {
		t.Fatalf("expected 0 reconciled for expired unconsumed offer, got %d", got)
	}
	if replayedAt.IsZero() || replayedAt.Before(expiry) {
		t.Fatalf("replayed too early: replayedAt=%v expiry=%v", replayedAt, expiry)
	}
}

func TestFleetPoolDecouplesZonesAndSubscriptionsWithoutDuplicates(t *testing.T) {
	const fleet = 400
	pool := newFleetPool(fleet, 4)
	seenScooters := make(map[int]bool, fleet)
	seenUsers := make(map[int]bool, fleet)
	type combo struct {
		zone    int
		sub     bool
		trusted bool
	}
	combos := make(map[combo]int)
	for range fleet {
		s, u, ok := pool.acquire()
		if !ok {
			t.Fatal("fleet unexpectedly exhausted")
		}
		if s < 1 || s > fleet || u < 1 || u > fleet {
			t.Fatalf("out-of-range pair (%d, %d)", s, u)
		}
		if seenScooters[s] || seenUsers[u] {
			t.Fatalf("duplicate active assignment (%d, %d)", s, u)
		}
		seenScooters[s] = true
		seenUsers[u] = true
		combos[combo{zone: s % 4, sub: u%4 == 0, trusted: u%3 == 0}]++
	}
	if _, _, ok := pool.acquire(); ok {
		t.Fatal("expected fleet exhaustion after acquiring all pairs")
	}
	// All 4 zones x 2 subscription states x 2 trust states = 16 combinations.
	if len(combos) != 16 {
		t.Fatalf("expected all 16 (zone, subscription, trusted) combinations, got %d: %v", len(combos), combos)
	}
	// Specifically check zone-1 (s%4==0) includes non-subscribers and zone-2..4 include subscribers.
	if combos[combo{zone: 0, sub: false, trusted: false}] == 0 {
		t.Fatalf("zone-1 never paired with a non-subscriber: %v", combos)
	}
	for z := 1; z < 4; z++ {
		if combos[combo{zone: z, sub: true, trusted: false}] == 0 {
			t.Fatalf("zone-%d never paired with a subscriber: %v", z+1, combos)
		}
	}
	pool.release(1, 2)
	if s, u, ok := pool.acquire(); !ok || s != 1 || u != 2 {
		t.Fatalf("release/acquire mismatch: (%d, %d, %t)", s, u, ok)
	}
}

func TestSimulateRejectsInvalidProfileCombinations(t *testing.T) {
	base := options{
		url: "http://127.0.0.1:1", rate: 1, reads: 50, fleet: 10,
		concurrency: 16, queueSize: 64, duration: time.Second,
		minRide: time.Second, maxRide: time.Second,
	}
	t.Run("history_file_without_historical", func(t *testing.T) {
		o := base
		o.historyFile = "some.ids"
		if _, err := simulate(o); err == nil {
			t.Fatal("expected error when -history-file is set with -historical=0")
		}
	})
	t.Run("historical_without_history_file", func(t *testing.T) {
		o := base
		o.historical = 15
		if _, err := simulate(o); err == nil {
			t.Fatal("expected error when -historical>0 without -history-file")
		}
	})
	t.Run("concurrency_exceeds_max", func(t *testing.T) {
		o := base
		o.concurrency = maxReadWorkers + 1
		if _, err := simulate(o); err == nil {
			t.Fatal("expected error when -read-concurrency exceeds maximum")
		}
	})
	t.Run("negative_clock_skew", func(t *testing.T) {
		o := base
		o.clockSkew = -time.Millisecond
		if _, err := simulate(o); err == nil {
			t.Fatal("expected error for negative -clock-skew")
		}
	})
}
