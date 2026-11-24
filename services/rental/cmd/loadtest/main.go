// Command loadtest drives independent arrivals and measures complete scooter trips.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type options struct {
	url, historyFile                                       string
	rate, reads, historical, fleet, concurrency, queueSize int
	warmup, duration, minRide, maxRide                     time.Duration
	seed                                                   int64
}

type response struct {
	OfferID    string     `json:"offer_id"`
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	Cost       *int64     `json:"cost"`
	MinuteRate *int64     `json:"minute_rate"`
	UnlockFee  *int64     `json:"unlock_fee"`
	Deposit    *int64     `json:"deposit"`
	StartedAt  *time.Time `json:"started_at"`
	FinishTime *time.Time `json:"finish_time"`
	ErrorCode  string     `json:"error_code"`
}

type run struct {
	opts                          options
	client                        *http.Client
	runID                         string
	start, from, to               time.Time
	history                       []string
	fleet                         chan int
	queue                         *readQueue
	rides, readers                sync.WaitGroup
	mu                            sync.Mutex
	stats                         map[string]*route
	failures                      map[string]int
	accepted, finished, scheduled int
	mismatchedRides               int
	audits                        []finishAudit
	unresolved                    []incomplete
}

func (r *run) record(name string, due time.Time, outcome string) {
	r.recordAt(name, due, time.Now(), outcome)
}

func (r *run) recordAt(name string, due, end time.Time, outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if outcome != "ok" {
		r.failures[name+"/"+outcome]++
	}
	route := r.stats[name]
	if outcome != "generator_saturated" && outcome != "fleet_exhausted" {
		route.totalSent++
	}
	if outcome == "ok" {
		route.totalValid++
		if name == "finish" {
			r.finished++
		}
	}
	if !end.Before(r.from) && end.Before(r.to) {
		route.latencies = append(route.latencies, end.Sub(due))
		if outcome == "ok" {
			route.success++
		}
	}
}

func (r *run) request(method, path string, body any, expected ...int) (response, string) {
	var out response
	var payload []byte
	if body != nil {
		payload, _ = json.Marshal(body)
	}
	req, err := http.NewRequest(method, r.opts.url+path, bytes.NewReader(payload))
	if err != nil {
		return out, "url"
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return out, "transport"
	}
	defer resp.Body.Close()
	err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Sprintf("%d/%s", resp.StatusCode, out.ErrorCode)
	}
	if !slices.Contains(expected, resp.StatusCode) {
		return out, fmt.Sprintf("unexpected_http_%d", resp.StatusCode)
	}
	if err != nil {
		return out, "invalid_json"
	}
	return out, "ok"
}

// Each ride owns its scooter and user until a successful finish. Ambiguous
// outcomes quarantine the pair for the rest of this run.
func (r *run) trip(n, id int, due time.Time, ride time.Duration) {
	b := map[string]string{"user_id": fmt.Sprintf("user-%d", id), "scooter_id": fmt.Sprintf("scooter-%d", id), "idempotency_id": fmt.Sprintf("%s-%d", r.runID, n)}
	offer, outcome := r.request("POST", "/offers", b, http.StatusCreated)
	if outcome == "ok" && offer.OfferID == "" {
		outcome = "missing_id"
	}
	quote, validTerms := quoteFor(offer)
	if outcome == "ok" && !validTerms {
		outcome = "invalid_offer_terms"
	}
	r.record("offer", due, outcome)
	if outcome != "ok" {
		r.fleet <- id
		return
	}
	started := time.Now()
	order, outcome := r.request("POST", "/orders", map[string]string{"offer_id": offer.OfferID}, http.StatusAccepted)
	if outcome == "ok" && (order.OrderID == "" || order.Status != "STARTING") {
		outcome = "invalid_start"
	}
	r.record("start", started, outcome)
	if outcome != "ok" {
		if ambiguousStart(outcome) {
			r.remember(incomplete{offer: offer.OfferID})
		}
		return
	}
	r.mu.Lock()
	r.accepted++
	r.mu.Unlock()
	accepted := time.Now()
	var sentCurrent, sentHistory int
	path := "/orders/" + order.OrderID
	// Spread historical reads evenly over the ride; select reproducible random IDs.
	random := rand.New(rand.NewPCG(uint64(r.opts.seed), uint64(n)))
	for i := 0; i < r.opts.reads; i++ {
		readDue := accepted.Add(ride * time.Duration(2*i+1) / time.Duration(2*r.opts.reads))
		time.Sleep(time.Until(readDue))
		if (i+1)*r.opts.historical/r.opts.reads > i*r.opts.historical/r.opts.reads {
			histID := r.history[random.IntN(len(r.history))]
			if r.read("/orders/"+histID, "get_history", histID, readDue, nil) {
				sentHistory++
			}
		} else {
			if r.read(path, "get_current", order.OrderID, readDue, quote) {
				sentCurrent++
			}
		}
	}
	if sentCurrent != r.opts.reads-r.opts.historical || sentHistory != r.opts.historical {
		r.mu.Lock()
		r.mismatchedRides++
		r.mu.Unlock()
	}
	finishDue := accepted.Add(ride)
	time.Sleep(time.Until(finishDue))
	closed, outcome := r.request("POST", path+"/finish", nil, http.StatusOK)
	end := time.Now()
	if outcome == "ok" && (closed.OrderID != order.OrderID || closed.FinishTime == nil || closed.Cost == nil ||
		(closed.Status != "COMPLETED" && closed.Status != "CANCELLED") || *closed.Cost < 0 || (closed.Status == "CANCELLED" && *closed.Cost != 0)) {
		outcome = "invalid_finish"
	}
	if outcome == "ok" {
		if start := quote.started.Load(); start == nil {
			r.mu.Lock()
			r.audits = append(r.audits, finishAudit{quote, closed, finishDue, end})
			r.mu.Unlock()
			r.fleet <- id
			return
		} else if !validFinal(closed, *start, quote) {
			outcome = "invalid_finish_cost"
		}
	}
	r.recordAt("finish", finishDue, end, outcome)
	if outcome != "ok" {
		r.remember(incomplete{order: order.OrderID})
		return
	}
	r.fleet <- id
}

type route struct {
	totalSent, totalValid int
	success               int
	latencies             []time.Duration
}
type routeSummary struct {
	Success  int     `json:"success"`
	Attempts int     `json:"attempts"`
	RPS      float64 `json:"success_rps"`
	P95MS    float64 `json:"p95_ms"`
}

func (s route) summarize(window time.Duration) routeSummary {
	var p95 time.Duration
	if n := len(s.latencies); n > 0 {
		slices.Sort(s.latencies)
		p95 = s.latencies[n-1-(n/20)]
	}
	return routeSummary{s.success, len(s.latencies), float64(s.success) / window.Seconds(), float64(p95) / float64(time.Millisecond)}
}

func simulate(o options) (bool, error) {
	if o.rate < 1 || o.reads < 1 || o.fleet < 1 || o.concurrency < 1 || o.queueSize < 1 || o.warmup < 0 || o.duration <= 0 || o.minRide <= 0 || o.maxRide < o.minRide || o.historical < 0 || o.historical > o.reads {
		return false, fmt.Errorf("invalid load profile")
	}
	r := &run{opts: o, runID: uuid.NewString(), fleet: make(chan int, o.fleet), stats: map[string]*route{}, failures: map[string]int{}}
	if o.historical > 0 {
		data, err := os.ReadFile(o.historyFile)
		if err != nil {
			return false, err
		}
		r.history = strings.Fields(string(data))
		if len(r.history) == 0 {
			return false, fmt.Errorf("empty historical ID list")
		}
		for _, id := range r.history {
			if _, err = uuid.Parse(id); err != nil {
				return false, err
			}
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// GET workers share this transport with offer/start/finish. Keep enough
	// idle sockets for both streams; admission of new trips remains independent.
	tr.MaxIdleConnsPerHost = min(o.concurrency, maxReadWorkers) + 128
	tr.MaxIdleConns = tr.MaxIdleConnsPerHost
	r.client = &http.Client{Timeout: 5 * time.Second, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer tr.CloseIdleConnections()
	for i := 1; i <= o.fleet; i++ {
		r.fleet <- i
	}
	for _, name := range []string{"offer", "start", "get_current", "get_history", "finish"} {
		r.stats[name] = &route{}
	}
	r.startReads(o.concurrency, o.queueSize)
	random := rand.New(rand.NewPCG(uint64(o.seed), 0))
	r.start = time.Now()
	r.from = r.start.Add(o.warmup)
	r.to = r.from.Add(o.duration)
	fmt.Fprintf(os.Stderr, "run %s, window %s .. %s\n", r.runID, r.from.UTC().Format(time.RFC3339Nano), r.to.UTC().Format(time.RFC3339Nano))
	stopProgress := r.startReadProgress(os.Stderr, time.Minute)
	for n := 0; ; n++ {
		due := r.start.Add(time.Duration(n) * time.Second / time.Duration(o.rate))
		if !due.Before(r.to) {
			break
		}
		time.Sleep(time.Until(due))
		r.scheduled++
		ride := o.minRide + time.Duration(random.Int64N(int64(o.maxRide-o.minRide)+1))
		select {
		case id := <-r.fleet:
			r.rides.Go(func() { r.trip(n, id, due, ride) })
		default:
			r.record("offer", due, "fleet_exhausted")
		}
	}
	r.rides.Wait()
	r.drainReads()
	stopProgress()
	auditGETs := r.finishAudits()
	repaired := r.reconcile()
	getTotals, readPass := r.readCounts()
	routes := map[string]routeSummary{}
	for name, s := range r.stats {
		routes[name] = s.summarize(o.duration)
	}
	result := map[string]any{
		"run_id": r.runID, "start": r.start.UTC(), "window_from": r.from.UTC(), "window_to": r.to.UTC(), "drain_seconds": time.Since(r.to).Seconds(),
		"rate": o.rate, "reads_per_ride": o.reads, "historical_reads_per_ride": o.historical, "history_ids": len(r.history), "fleet": o.fleet, "seed": o.seed,
		"min_ride_seconds": o.minRide.Seconds(), "max_ride_seconds": o.maxRide.Seconds(), "scheduled_trips": r.scheduled,
		"accepted_trips": r.accepted, "primary_gets": getTotals, "rides_with_wrong_get_distribution": r.mismatchedRides,
		"read_queue": r.queue.stats, "primary_gets_pass": readPass, "post_drain_finish_audit_gets": auditGETs,
		"completed_trips": r.finished, "ambiguous_trips": len(r.unresolved), "reconciled_after_drain": repaired, "all_failures": r.failures, "window_routes": routes,
	}
	// The steady-state RPS tolerance is specified by docs/TEST_PLAN.md.
	pass := readPass && len(r.failures) == 0 && r.finished == r.scheduled
	for name, target := range map[string]int{"offer": o.rate, "start": o.rate, "finish": o.rate, "get_current": o.rate * (o.reads - o.historical), "get_history": o.rate * o.historical} {
		route := routes[name]
		limit := map[string]float64{"offer": 1000, "start": 2000, "finish": 2000, "get_current": 300, "get_history": 300}[name]
		if target > 0 && (route.RPS < float64(target)*0.95 || route.RPS > float64(target)*1.05 || route.P95MS > limit) {
			pass = false
		}
	}
	result["http_profile_pass"] = pass
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return pass, enc.Encode(result)
}

func main() {
	var o options
	flag.StringVar(&o.url, "url", "http://127.0.0.1:18080", "Rental API URL")
	flag.IntVar(&o.rate, "rate", 100, "open-loop trip arrivals per second")
	flag.IntVar(&o.reads, "reads", 50, "scheduled GETs per accepted trip")
	flag.IntVar(&o.historical, "historical", 0, "GETs per trip directed to historical IDs")
	flag.StringVar(&o.historyFile, "history-file", "", "whitespace-separated historical UUIDs")
	flag.DurationVar(&o.duration, "window", 30*time.Minute, "measured interval")
	flag.DurationVar(&o.warmup, "warmup", 7*time.Minute, "unmeasured ramp-up")
	flag.DurationVar(&o.minRide, "min-ride", 4*time.Minute, "minimum ride duration")
	flag.DurationVar(&o.maxRide, "max-ride", 6*time.Minute, "maximum ride duration")
	flag.IntVar(&o.fleet, "fleet", 40000, "number of pairs seeded by Source")
	flag.IntVar(&o.concurrency, "read-concurrency", 512, "GET network workers (capped at 1024)")
	flag.IntVar(&o.queueSize, "read-queue", 32768, "maximum queued primary GETs before an explicit generator_saturated failure")
	flag.Int64Var(&o.seed, "seed", 4, "trip duration and historical selection seed")
	flag.Parse()
	o.url = strings.TrimRight(o.url, "/")
	pass, err := simulate(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if !pass {
		os.Exit(1)
	}
}
