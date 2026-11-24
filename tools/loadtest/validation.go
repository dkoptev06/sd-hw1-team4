package main

import (
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Use the offer returned over HTTP as the client's quotation. The SQL audit
// independently checks those terms against the seeded Source catalog.
type quotedTrip struct {
	minuteRate int64
	unlockFee  int64
	started    atomic.Pointer[time.Time]
}

func quoteFor(offer response) (*quotedTrip, bool) {
	if offer.MinuteRate == nil || offer.UnlockFee == nil || offer.Deposit == nil ||
		offer.ExpiresAt == nil || offer.ExpiresAt.IsZero() ||
		*offer.MinuteRate < 0 || *offer.UnlockFee < 0 || *offer.Deposit < 0 {
		return nil, false
	}
	return &quotedTrip{minuteRate: *offer.MinuteRate, unlockFee: *offer.UnlockFee}, true
}

// bill implements the tariff as whole minutes plus the fractional last minute,
// without overflowing when a representable total has a large per-minute rate.
func (q *quotedTrip) bill(seconds int64) (int64, bool) {
	if seconds < 5 {
		return 0, true
	}
	whole, tail := seconds/60, seconds%60
	fraction := (q.minuteRate/60)*tail + (q.minuteRate%60)*tail/60
	if fraction > math.MaxInt64-q.unlockFee {
		return 0, false
	}
	base := q.unlockFee + fraction
	if q.minuteRate != 0 && whole > (math.MaxInt64-base)/q.minuteRate {
		return 0, false
	}
	return base + whole*q.minuteRate, true
}

func (q *quotedTrip) observeStarted(at *time.Time) bool {
	if q.started.CompareAndSwap(nil, at) {
		return true
	}
	return q.started.Load().Equal(*at)
}

// By default the driver and PostgreSQL share the Compose host clock (DB
// timestamps have microsecond precision). Optional skew allows cross-host runs.
// A valid cost must be attainable at some whole second between the actual HTTP
// send and receive, including the five-second boundary.
func (q *quotedTrip) liveCost(cost int64, start, sent, received time.Time, skew time.Duration) bool {
	if skew < 0 {
		skew = 0
	}
	if cost < 0 || start.After(received.Add(skew)) {
		return false
	}
	lo := max(int64(0), int64(sent.Add(-skew).Sub(start)/time.Second))
	hi := int64(received.Add(skew).Sub(start) / time.Second)
	for lo <= hi {
		mid := lo + (hi-lo)/2
		value, ok := q.bill(mid)
		if ok && value == cost {
			return true
		}
		if ok && value < cost {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return false
}

func validFinal(v response, start time.Time, q *quotedTrip) bool {
	if v.FinishTime == nil || v.Cost == nil || *v.Cost < 0 || v.FinishTime.Before(start) {
		return false
	}
	elapsed := v.FinishTime.Sub(start)
	if elapsed < 5*time.Second {
		return v.Status == "CANCELLED" && *v.Cost == 0
	}
	if v.Status != "COMPLETED" {
		return false
	}
	if q == nil {
		return true
	} // imported history IDs have no supplied offer terms
	cost, ok := q.bill(int64(elapsed / time.Second))
	return ok && *v.Cost == cost
}

func validView(v response, id string, q *quotedTrip, sent, received time.Time, skew time.Duration) bool {
	if v.OrderID != id {
		return false
	}
	switch v.Status {
	case "STARTING":
		return q != nil && v.Cost == nil && v.StartedAt == nil && v.FinishTime == nil
	case "START_FAILED":
		return q == nil && v.Cost != nil && *v.Cost == 0 && v.StartedAt == nil && v.FinishTime == nil
	case "ACTIVE":
		return q != nil && v.StartedAt != nil && v.FinishTime == nil && v.Cost != nil &&
			q.liveCost(*v.Cost, *v.StartedAt, sent, received, skew) && q.observeStarted(v.StartedAt)
	case "COMPLETED", "CANCELLED":
		return v.StartedAt != nil && validFinal(v, *v.StartedAt, q) && (q == nil || q.observeStarted(v.StartedAt))
	default:
		return false
	}
}

type finishAudit struct {
	quote    *quotedTrip
	closed   response
	due, end time.Time // original HTTP completion, even if validated after drain
}

// finish omits started_at. Every primary current GET can supply it. If a
// trip had no such observation (e.g. all 50 GETs targeted imported history),
// retrieve its final view after the workload, outside the measured requests.
func (r *run) finishAudits() int64 {
	if len(r.audits) == 0 {
		return 0
	}
	var extraReads atomic.Int64
	jobs := make(chan finishAudit)
	var workers sync.WaitGroup
	for range min(16, len(r.audits)) {
		workers.Go(func() {
			for a := range jobs {
				start := a.quote.started.Load()
				result := "ok"
				if start == nil {
					extraReads.Add(1)
					view, outcome := r.request("GET", "/orders/"+a.closed.OrderID, nil, http.StatusOK)
					switch {
					case outcome != "ok":
						result = "audit_" + outcome
					case view.OrderID != a.closed.OrderID || view.Status != a.closed.Status ||
						view.StartedAt == nil || view.FinishTime == nil || !view.FinishTime.Equal(*a.closed.FinishTime) ||
						view.Cost == nil || *view.Cost != *a.closed.Cost:
						result = "inconsistent_finish_view"
					default:
						start = view.StartedAt
					}
				}
				if result == "ok" && !validFinal(a.closed, *start, a.quote) {
					result = "invalid_finish_cost"
				}
				r.recordAt("finish", a.due, a.end, result)
			}
		})
	}
	for _, a := range r.audits {
		jobs <- a
	}
	close(jobs)
	workers.Wait()
	return extraReads.Load()
}

type getCounts struct {
	Expected  int `json:"expected"`
	Sent      int `json:"sent"`
	Validated int `json:"validated"`
}

func (r *run) readCounts() (map[string]getCounts, bool) {
	valid := r.mismatchedRides == 0
	counts := make(map[string]getCounts, 2)
	for name, perRide := range map[string]int{"get_current": r.opts.reads - r.opts.historical, "get_history": r.opts.historical} {
		s := r.stats[name]
		v := getCounts{r.accepted * perRide, s.totalSent, s.totalValid}
		counts[name] = v
		valid = valid && v.Expected == v.Sent && v.Expected == v.Validated
	}
	return counts, valid
}
