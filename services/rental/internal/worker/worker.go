// Package worker executes START, SETTLE and VOID tasks with at-least-once semantics.
//
// A dispatcher claims due tasks (claim_id + lease) in a short transaction, an executor
// calls Payment outside any transaction and then records the result only if the claim
// is still valid. Sweeps close START tasks past their deadline and tasks that ran out
// of attempts, independently of leases and schedules.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

type Payments interface {
	Hold(context.Context, task.HoldParams) task.Result
	Settle(context.Context, task.SettleParams) task.Result
	Void(context.Context, task.VoidParams) task.Result
}

// Options configures dispatch, sweeping, and graceful shutdown.
type Options struct {
	Concurrency     int
	BatchSize       int
	PollInterval    time.Duration
	SweepInterval   time.Duration
	ShutdownTimeout time.Duration
}

type Worker struct {
	orders   *order.Service
	tasks    *task.Service
	payments Payments
	cfg      Options
	log      *slog.Logger
}

func New(orders *order.Service, tasks *task.Service, payments Payments, cfg Options, log *slog.Logger) *Worker {
	// Export a zero sample so Prometheus can detect the first DLQ transition.
	for _, kind := range []task.Kind{task.Settle, task.Void} {
		metrics.TasksFinished.WithLabelValues(string(kind), string(task.DeadLetter), "")
	}
	return &Worker{orders: orders, tasks: tasks, payments: payments, cfg: cfg, log: log}
}

// Run processes tasks until ctx is done, then waits for in-flight attempts.
func (w *Worker) Run(ctx context.Context) error {
	// In-flight attempts finish on shutdown so their results are not lost to the lease.
	execCtx := context.WithoutCancel(ctx)
	slots := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup

	inflight.Go(func() { w.sweepLoop(ctx) })

	for ctx.Err() == nil {
		n := w.acquireSlots(ctx, slots)
		if n == 0 {
			break
		}
		claimed, err := w.tasks.Claim(ctx, n)
		if err != nil && ctx.Err() == nil {
			w.log.Error("claim tasks", "error", err)
		}
		for range n - len(claimed) {
			<-slots
		}
		for _, task := range claimed {
			inflight.Go(func() {
				defer func() { <-slots }()
				w.execute(execCtx, task)
			})
		}
		if len(claimed) == 0 {
			sleep(ctx, w.cfg.PollInterval)
		}
	}

	done := make(chan struct{})
	go func() { inflight.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(w.cfg.ShutdownTimeout):
		return errors.New("worker shutdown timed out with attempts in flight")
	}
}

// acquireSlots blocks for one free executor slot and then takes up to BatchSize-1 more without blocking.
func (w *Worker) acquireSlots(ctx context.Context, slots chan struct{}) int {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < w.cfg.BatchSize {
		select {
		case slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func (w *Worker) execute(ctx context.Context, claim task.Claim) {
	kind := string(claim.Kind())
	metrics.TaskClaimLag.WithLabelValues(kind).Observe(claim.Lag.Seconds())
	var hold *task.PreparedHold
	var err error
	if claim.Kind() == task.Start {
		hold, err = w.orders.PrepareStart(ctx, claim)
		if err != nil {
			w.log.Error("check START claim before HOLD", "task_id", claim.ID(), "order_id", claim.OrderID(), "error", err)
			return
		}
		if hold == nil {
			return
		}
	}

	started := time.Now()
	result := w.call(ctx, claim, hold)
	metrics.TaskAttemptDuration.WithLabelValues(kind).Observe(time.Since(started).Seconds())
	metrics.TaskAttempts.WithLabelValues(kind, result.Outcome.String()).Inc()

	var event *task.Event
	if claim.Kind() == task.Start {
		event, err = w.orders.ApplyStart(ctx, claim, result)
	} else {
		event, err = w.tasks.ApplyPayment(ctx, claim, result)
	}
	if err != nil {
		// The lease expires and the attempt is retried; Payment deduplicates by operation_id.
		w.log.Error("record task attempt", "task_id", claim.ID(), "order_id", claim.OrderID(), "kind", kind, "error", err)
		return
	}
	w.report(event)
}

func (w *Worker) call(ctx context.Context, claim task.Claim, hold *task.PreparedHold) task.Result {
	switch claim.Kind() {
	case task.Start:
		if hold.Detail != "" {
			return task.Result{Outcome: task.ContractError, Detail: hold.Detail}
		}
		if hold.Params.Amount == 0 {
			return task.Result{Outcome: task.Success}
		}
		return w.payments.Hold(ctx, hold.Params)
	case task.Settle:
		params, err := w.tasks.SettleParams(claim)
		if err != nil {
			return task.Result{Outcome: task.ContractError, Detail: err.Error()}
		}
		return w.payments.Settle(ctx, params)
	case task.Void:
		params, err := w.tasks.VoidParams(claim)
		if err != nil {
			return task.Result{Outcome: task.ContractError, Detail: err.Error()}
		}
		return w.payments.Void(ctx, params)
	default:
		return task.Result{Outcome: task.ContractError, Detail: fmt.Sprintf("invalid task parameters: unknown task kind %q", claim.Kind())}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
