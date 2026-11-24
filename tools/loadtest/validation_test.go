package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func amount(n int64) *int64 { return &n }

func TestActiveQuoteIsAttainableDuringActualRequest(t *testing.T) {
	q := &quotedTrip{minuteRate: 120, unlockFee: 50}
	start := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		lo, hi time.Duration
		cost   int64
		valid  bool
	}{
		{4 * time.Second, 5 * time.Second, 0, true},
		{4 * time.Second, 5 * time.Second, 60, true},
		{4 * time.Second, 5 * time.Second, 42, false}, // unlock is a discontinuity
		{5 * time.Second, 7 * time.Second, 62, true},
		{5 * time.Second, 7 * time.Second, 61, false}, // the rate adds two units per second
		{15 * time.Second, 15*time.Second + time.Millisecond, 0, false},
		{-time.Second, -time.Millisecond, 0, false}, // start is in the future
	} {
		if got := q.liveCost(tc.cost, start, start.Add(tc.lo), start.Add(tc.hi), time.Microsecond); got != tc.valid {
			t.Errorf("interval %v..%v with cost %d accepted=%t", tc.lo, tc.hi, tc.cost, got)
		}
	}
	// With configurable clock skew, a 100ms host-vs-DB drift across a second boundary is accepted.
	sent, recv := start.Add(5*time.Second+50*time.Millisecond), start.Add(5*time.Second+70*time.Millisecond)
	if q.liveCost(0, start, sent, recv, time.Microsecond) {
		t.Fatal("cost from 4s accepted under 1us skew at 5.05s")
	}
	if !q.liveCost(0, start, sent, recv, 100*time.Millisecond) {
		t.Fatal("cost from 4s rejected under 100ms skew at 5.05s")
	}
}

func TestQuoteFieldsAndExactFinalPrice(t *testing.T) {
	exp := time.Now().UTC().Add(2 * time.Minute)
	offer := response{MinuteRate: amount(17), UnlockFee: amount(50), Deposit: amount(0), ExpiresAt: &exp}
	q, ok := quoteFor(offer)
	if !ok {
		t.Fatal("valid response rejected")
	}
	noExp := offer
	noExp.ExpiresAt = nil
	if _, ok := quoteFor(noExp); ok {
		t.Fatal("missing expires_at accepted")
	}
	offer.MinuteRate = nil
	if _, ok := quoteFor(offer); ok {
		t.Fatal("missing rate accepted")
	}
	start := time.Now().UTC()
	for _, tc := range []struct {
		dt     time.Duration
		status string
		cost   int64
		valid  bool
	}{
		{5*time.Second - time.Microsecond, "CANCELLED", 0, true},
		{5 * time.Second, "COMPLETED", 51, true},
		{65*time.Second + time.Millisecond, "COMPLETED", 68, true},
		{65 * time.Second, "COMPLETED", 67, false},
		{65 * time.Second, "CANCELLED", 0, false},
		{-time.Second, "CANCELLED", 0, false},
	} {
		end := start.Add(tc.dt)
		view := response{Status: tc.status, Cost: amount(tc.cost), FinishTime: &end}
		if validFinal(view, start, q) != tc.valid {
			t.Errorf("%+v", tc)
		}
	}
	large := &quotedTrip{minuteRate: math.MaxInt64}
	if bill, ok := large.bill(60); !ok || bill != math.MaxInt64 {
		t.Fatalf("large valid bill: %d %t", bill, ok)
	}
	if _, ok := large.bill(61); ok {
		t.Fatal("unrepresentable bill accepted")
	}
}

func TestStartingAmountAndImportedHistory(t *testing.T) {
	now := time.Now().UTC()
	q := &quotedTrip{minuteRate: 17}
	for _, body := range []string{`{"order_id":"a","status":"STARTING"}`, `{"order_id":"a","status":"STARTING","cost":null}`} {
		var v response
		if err := json.Unmarshal([]byte(body), &v); err != nil || !validView(v, "a", q, now, now, time.Microsecond) {
			t.Fatalf("%s %v", body, err)
		}
	}
	if validView(response{OrderID: "a", Status: "STARTING", Cost: amount(0)}, "a", q, now, now, time.Microsecond) {
		t.Fatal("numeric STARTING price accepted")
	}
	start := now.Add(-2 * time.Minute)
	v := response{OrderID: "a", Status: "COMPLETED", StartedAt: &start, FinishTime: &now, Cost: amount(123456)}
	if !validView(v, "a", nil, now, now, time.Microsecond) {
		t.Fatal("arbitrary historical offer wrongly compared to current terms")
	}
	if validView(v, "a", q, now, now, time.Microsecond) {
		t.Fatal("wrong completed current price accepted")
	}
	// Historical CANCELLED and START_FAILED views are also valid archived responses.
	shortEnd := start.Add(2 * time.Second)
	if !validView(response{OrderID: "c", Status: "CANCELLED", StartedAt: &start, FinishTime: &shortEnd, Cost: amount(0)}, "c", nil, now, now, time.Microsecond) {
		t.Fatal("valid archived CANCELLED rejected")
	}
	if !validView(response{OrderID: "f", Status: "START_FAILED", Cost: amount(0)}, "f", nil, now, now, time.Microsecond) {
		t.Fatal("valid archived START_FAILED rejected")
	}
}

func TestPrimaryHTTPStatusContract(t *testing.T) {
	for _, tc := range []struct {
		method          string
		expected, wrong int
	}{
		{"POST", 201, 200}, {"POST", 202, 200}, {"GET", 200, 201}, {"POST", 200, 202},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(tc.wrong)
			_, _ = w.Write([]byte(`{"status":"STARTING"}`))
		}))
		r := run{opts: options{url: server.URL}, client: server.Client()}
		_, result := r.request(tc.method, "/test", nil, tc.expected)
		server.Close()
		if result != "unexpected_http_"+strconv.Itoa(tc.wrong) {
			t.Fatalf("%+v: %s", tc, result)
		}
	}
}

func TestPrimaryGETCountsIncludeWarmupAndDrain(t *testing.T) {
	from := time.Now()
	r := run{opts: options{reads: 50, historical: 15}, accepted: 1, from: from, to: from.Add(time.Minute),
		failures: map[string]int{}, stats: map[string]*route{"get_current": {}, "get_history": {}}}
	for i := range 50 {
		at := from.Add(time.Duration(i-10) * 2 * time.Second)
		name := "get_current"
		if i >= 35 {
			name = "get_history"
		}
		r.recordAt(name, at, at, "ok")
	}
	got, pass := r.readCounts()
	if !pass || got["get_current"] != (getCounts{35, 35, 35}) || got["get_history"] != (getCounts{15, 15, 15}) {
		t.Fatalf("%+v pass=%t", got, pass)
	}
	r.mismatchedRides = 1
	if _, pass := r.readCounts(); pass {
		t.Fatal("wrong per-ride distribution accepted")
	}
	r.mismatchedRides = 0
	r.accepted = 2
	r.recordAt("get_current", from, from, "generator_saturated")
	got, pass = r.readCounts()
	if pass || got["get_current"].Sent != 35 {
		t.Fatalf("skipped GET counted as sent: %+v", got)
	}
}

func TestFinishAuditValidatesAtOriginalCompletionTime(t *testing.T) {
	start := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	view := response{OrderID: "r", Status: "COMPLETED", Cost: amount(67), StartedAt: &start, FinishTime: &end}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/orders/r" {
			t.Errorf("unexpected path %s", req.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(view)
	}))
	defer server.Close()
	closed := view
	closed.StartedAt = nil // contract of POST finish
	r := run{opts: options{url: server.URL}, client: server.Client(), from: start, to: end.Add(time.Second),
		stats: map[string]*route{"finish": {}}, failures: map[string]int{},
		audits: []finishAudit{{&quotedTrip{minuteRate: 17, unlockFee: 50}, closed, end.Add(-time.Millisecond), end}}}
	if n := r.finishAudits(); n != 1 || r.stats["finish"].success != 1 || r.finished != 1 || r.stats["finish"].latencies[0] != time.Millisecond {
		t.Fatalf("reads=%d stats=%+v failures=%v", n, r.stats["finish"], r.failures)
	}
	r.audits[0].closed.Cost = amount(999)
	r.finishAudits()
	if r.finished != 1 || r.failures["finish/inconsistent_finish_view"] != 1 {
		t.Fatalf("disagreement hidden: %+v", r.failures)
	}
}
