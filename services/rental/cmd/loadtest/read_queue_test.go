package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Hold the workers at the HTTP endpoint while the producer fills the ring.
func blockedReads(t *testing.T, workers, capacity int) (*run, <-chan string, func()) {
	t.Helper()
	entered := make(chan string, workers+capacity)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		id := strings.TrimPrefix(req.URL.Path, "/orders/")
		entered <- id
		<-release
		fmt.Fprintf(w, `{"order_id":%q,"status":"COMPLETED","cost":22,"started_at":"2026-11-23T23:58:00Z","finish_time":"2026-11-24T00:00:00Z"}`, id)
	}))
	now := time.Now()
	r := &run{opts: options{url: srv.URL}, client: srv.Client(), from: now.Add(-time.Minute), to: now.Add(time.Minute),
		stats: map[string]*route{"get_history": {}, "get_current": {}}, failures: map[string]int{}}
	r.startReads(workers, capacity)
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { resume(); r.drainReads(); srv.Close() })
	return r, entered, resume
}

func nextHTTPRead(t *testing.T, entered <-chan string) string {
	t.Helper()
	select {
	case id := <-entered:
		return id
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not dispatch the expected GET")
		return ""
	}
}

func TestReadQueueFIFOOverflowAndDrain(t *testing.T) {
	r, entered, resume := blockedReads(t, 1, 2)
	due := time.Now().Add(-75 * time.Millisecond)
	enqueue := func(id string) bool { return r.read("/orders/"+id, "get_history", id, due, nil) }
	if !enqueue("first") || nextHTTPRead(t, entered) != "first" {
		t.Fatal("first GET did not reach the endpoint")
	}
	if !enqueue("second") || !enqueue("third") || enqueue("rejected") {
		t.Fatal("a bounded queue must accept two waiting GETs and report the third enqueue as a failure")
	}
	var progress bytes.Buffer
	r.writeReadProgress(&progress)
	if !strings.Contains(progress.String(), "queue=2/2 peak=2 overflows=1 sent=1 validated=0") {
		t.Fatalf("progress during backlog: %s", progress.String())
	}
	done := make(chan struct{})
	go func() { r.drainReads(); close(done) }()
	select {
	case <-done:
		t.Fatal("drain returned with three requests outstanding")
	case <-time.After(time.Millisecond):
	}
	resume()
	for _, want := range []string{"second", "third"} {
		if got := nextHTTPRead(t, entered); got != want {
			t.Fatalf("GET %s reached endpoint before %s", got, want)
		}
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("workers failed to drain accepted GETs")
	}
	s := r.stats["get_history"]
	if s.totalSent != 3 || s.totalValid != 3 || r.failures["get_history/generator_saturated"] != 1 {
		t.Fatalf("route=%+v failures=%v", s, r.failures)
	}
	if got := s.summarize(time.Second); got.Attempts != 4 || got.P95MS < 75 {
		t.Fatalf("latency must include wait from original due time: %+v", got)
	}
	q := r.queue.stats
	if q.PeakQueued != 2 || q.Overflows != 1 || q.Dispatched != 3 || q.MaxDispatchDelayMS < 75 {
		t.Fatalf("queue stats %+v", q)
	}
	progress.Reset()
	r.writeReadProgress(&progress)
	if !strings.Contains(progress.String(), "queue=0/2 peak=2 overflows=1 sent=3 validated=3") {
		t.Fatalf("progress after drain: %s", progress.String())
	}
}

func TestReadQueueAbsorbsShortBurstBeyondWorkerConcurrency(t *testing.T) {
	const workers, capacity = 2, 8
	r, entered, resume := blockedReads(t, workers, capacity)
	for i := range workers + capacity {
		id := fmt.Sprint(i)
		if !r.read("/orders/"+id, "get_history", id, time.Now(), nil) {
			t.Fatalf("dropped request %d despite queue room", i)
		}
		if i < workers && nextHTTPRead(t, entered) != id {
			t.Fatalf("worker did not start GET %d", i)
		}
	}
	select {
	case id := <-entered:
		t.Fatalf("GET %s exceeded worker concurrency while both workers were blocked", id)
	default:
	}
	resume()
	r.drainReads()
	if q := r.queue.stats; q.PeakQueued != capacity || q.Overflows != 0 || q.Dispatched != workers+capacity ||
		r.stats["get_history"].totalValid != workers+capacity || len(r.failures) != 0 {
		t.Fatalf("queue=%+v validated=%d failures=%v", q, r.stats["get_history"].totalValid, r.failures)
	}
}
