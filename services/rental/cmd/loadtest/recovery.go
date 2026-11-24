package main

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type incomplete struct{ offer, order string }

func ambiguousStart(outcome string) bool {
	return outcome == "transport" || outcome == "invalid_json" || outcome == "invalid_start" || strings.HasPrefix(outcome, "5") || strings.HasPrefix(outcome, "unexpected_http_")
}

func (r *run) remember(trip incomplete) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unresolved = append(r.unresolved, trip)
}

// Recover potentially accepted requests with their original identifiers. These
// calls run after measurement and never convert failed attempts into successes.
func (r *run) reconcile() int {
	if len(r.unresolved) == 0 {
		return 0
	}
	queue := make(chan incomplete, len(r.unresolved))
	for _, trip := range r.unresolved {
		queue <- trip
	}
	close(queue)
	var done atomic.Int64
	var workers sync.WaitGroup
	deadline := time.Now().Add(2 * time.Minute)
	for range min(16, len(r.unresolved)) {
		workers.Go(func() {
			for trip := range queue {
				if !time.Now().Before(deadline) {
					return
				}
				if r.closeTrip(trip, deadline) {
					done.Add(1)
				}
			}
		})
	}
	workers.Wait()
	return int(done.Load())
}

func (r *run) closeTrip(trip incomplete, deadline time.Time) bool {
	if trip.order == "" {
		replay, outcome := r.request("POST", "/orders", map[string]string{"offer_id": trip.offer}, http.StatusOK, http.StatusAccepted)
		if outcome != "ok" || replay.OrderID == "" {
			return false
		}
		trip.order = replay.OrderID
	}
	for time.Now().Before(deadline) {
		view, outcome := r.request("GET", "/orders/"+trip.order, nil, http.StatusOK)
		if outcome == "ok" {
			switch view.Status {
			case "START_FAILED", "COMPLETED", "CANCELLED":
				return true
			case "ACTIVE":
				final, outcome := r.request("POST", "/orders/"+trip.order+"/finish", nil, http.StatusOK)
				if outcome == "ok" && (final.Status == "COMPLETED" || final.Status == "CANCELLED") {
					return true
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}
