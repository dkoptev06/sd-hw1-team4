package main

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type incomplete struct {
	offer        string
	order        string
	offerExpires *time.Time
}

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
	now := time.Now()
	deadline := now.Add(2 * time.Minute)
	maxDeadline := now.Add(4 * time.Minute)
	for _, trip := range r.unresolved {
		if trip.order == "" && trip.offerExpires != nil {
			needed := trip.offerExpires.Add(30 * time.Second)
			if needed.After(deadline) {
				deadline = minTime(needed, maxDeadline)
			}
		}
	}
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
		// Wait past offer expiration before replaying POST /orders so an
		// unconsumed offer returns 409 OFFER_EXPIRED instead of creating a new
		// order after drain, while an already-created order still replays 200 OK.
		if trip.offerExpires != nil {
			waitUntil := minTime(trip.offerExpires.Add(100*time.Millisecond), deadline)
			if wait := time.Until(waitUntil); wait > 0 {
				time.Sleep(wait)
			}
			if !time.Now().Before(deadline) {
				return false
			}
		}
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

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
