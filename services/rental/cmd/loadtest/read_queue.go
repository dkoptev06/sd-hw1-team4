package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const maxReadWorkers = 1024

type scheduledRead struct {
	path, route, id string
	due             time.Time
	quote           *quotedTrip
}

type readQueueStats struct {
	Capacity           int     `json:"capacity"`
	Workers            int     `json:"workers"`
	PeakQueued         int     `json:"peak_queued"`
	Overflows          int     `json:"overflows"`
	Dispatched         int     `json:"dispatched"`
	MaxDispatchDelayMS float64 `json:"max_dispatch_delay_ms"`
}

// A bounded FIFO shared by trip producers and a fixed pool of HTTP workers.
// Producers never wait for space or for a response. Queue depth is sampled
// under the same lock as enqueue/dequeue, so the high-water mark is exact.
type readQueue struct {
	mu         sync.Mutex
	ready      *sync.Cond
	entries    []scheduledRead
	head, size int
	closed     bool
	stats      readQueueStats
}

func (r *run) startReads(workers, capacity int) {
	workers = min(workers, maxReadWorkers)
	q := &readQueue{entries: make([]scheduledRead, capacity), stats: readQueueStats{Workers: workers, Capacity: capacity}}
	q.ready = sync.NewCond(&q.mu)
	r.queue = q
	for range workers {
		r.readers.Go(r.readWorker)
	}
}

func (r *run) read(path, name, id string, due time.Time, quote *quotedTrip) bool {
	if r.queue.enqueue(scheduledRead{path, name, id, due, quote}) {
		return true
	}
	r.record(name, due, "generator_saturated")
	return false
}

func (q *readQueue) enqueue(req scheduledRead) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == len(q.entries) {
		q.stats.Overflows++
		return false
	}
	q.entries[(q.head+q.size)%len(q.entries)] = req
	q.size++
	q.stats.PeakQueued = max(q.stats.PeakQueued, q.size)
	q.ready.Signal()
	return true
}

func (q *readQueue) next() (scheduledRead, time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size == 0 && !q.closed {
		q.ready.Wait()
	}
	if q.size == 0 {
		return scheduledRead{}, time.Time{}, false
	}
	req := q.entries[q.head]
	q.entries[q.head] = scheduledRead{} // release the ride and its strings after dispatch
	q.head = (q.head + 1) % len(q.entries)
	q.size--
	sent := time.Now()
	q.stats.Dispatched++
	q.stats.MaxDispatchDelayMS = max(q.stats.MaxDispatchDelayMS, float64(sent.Sub(req.due))/float64(time.Millisecond))
	return req, sent, true
}

func (r *run) readWorker() {
	for {
		req, sent, ok := r.queue.next()
		if !ok {
			return
		}
		view, result := r.request("GET", req.path, nil, http.StatusOK)
		end := time.Now()
		if result == "ok" && !validView(view, req.id, req.quote, sent, end) {
			result = "invalid_order"
		}
		r.recordAt(req.route, req.due, end, result)
	}
}

// All trip producers must have finished before the queue is closed.
func (r *run) drainReads() {
	q := r.queue
	q.mu.Lock()
	q.closed = true
	q.ready.Broadcast()
	q.mu.Unlock()
	r.readers.Wait()
}

// Report throughout arrival and drain, including requests that have been
// dispatched but whose HTTP responses are still outstanding.
func (r *run) startReadProgress(w io.Writer, interval time.Duration) func() {
	ticker := time.NewTicker(interval)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.writeReadProgress(w)
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop); <-done }
}

func (r *run) writeReadProgress(w io.Writer) {
	q := r.queue
	q.mu.Lock()
	stats, depth := q.stats, q.size
	r.mu.Lock()
	valid := r.stats["get_current"].totalValid + r.stats["get_history"].totalValid
	r.mu.Unlock()
	q.mu.Unlock()
	fmt.Fprintf(w, "%s GET queue=%d/%d peak=%d overflows=%d sent=%d validated=%d max_dispatch_delay_ms=%.1f\n",
		time.Now().UTC().Format(time.RFC3339), depth, stats.Capacity, stats.PeakQueued, stats.Overflows, stats.Dispatched, valid, stats.MaxDispatchDelayMS)
}
