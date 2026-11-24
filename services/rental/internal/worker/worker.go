// Package worker executes START, SETTLE and VOID tasks with at-least-once semantics.
//
// A dispatcher claims due tasks (claim_id + lease) in a short transaction, an executor
// calls Payment outside any transaction and then records the result only if the claim
// is still valid. Sweeps close START tasks past their deadline and tasks that ran out
// of attempts, independently of leases and schedules.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/payment"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

// Payments executes Payment commands.
type Payments interface {
	Hold(ctx context.Context, p domain.HoldParams) payment.Result
	Settle(ctx context.Context, p domain.SettleParams) payment.Result
	Void(ctx context.Context, p domain.VoidParams) payment.Result
}

// Configs provides the retention used when a failed start closes an order.
type Configs interface {
	Current() dynconfig.Snapshot
}

type Worker struct {
	store       *storage.Store
	payments    Payments
	configs     Configs
	cfg         settings.Worker
	maxAttempts storage.MaxAttempts
	log         *slog.Logger
	jitter      func() float64
}

func New(store *storage.Store, payments Payments, configs Configs, cfg settings.Worker, log *slog.Logger) *Worker {
	// Export a zero sample so Prometheus can detect the first DLQ transition.
	for _, kind := range []domain.TaskKind{domain.TaskSettle, domain.TaskVoid} {
		metrics.TasksFinished.WithLabelValues(string(kind), string(domain.TaskDeadLetter), "")
	}
	return &Worker{
		store: store, payments: payments, configs: configs, cfg: cfg,
		maxAttempts: storage.MaxAttempts(cfg.MaxAttempts), log: log, jitter: rand.Float64,
	}
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
		claimed, err := w.store.ClaimTasks(ctx, n, w.cfg.Lease, w.maxAttempts)
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

func (w *Worker) execute(ctx context.Context, task storage.ClaimedTask) {
	kind := string(task.Kind)
	metrics.TaskClaimLag.WithLabelValues(kind).Observe(task.Lag.Seconds())
	var start *startAttempt
	var err error
	if task.Kind == domain.TaskStart {
		start, err = w.prepareStart(ctx, task.Task)
		if err != nil {
			w.log.Error("check START claim before HOLD", "task_id", task.ID, "order_id", task.OrderID, "error", err)
			return
		}
		if start == nil {
			return
		}
	}

	started := time.Now()
	result := w.call(ctx, task.Task, start)
	metrics.TaskAttemptDuration.WithLabelValues(kind).Observe(time.Since(started).Seconds())
	metrics.TaskAttempts.WithLabelValues(kind, result.Outcome.String()).Inc()

	if task.Kind == domain.TaskStart {
		err = w.applyStart(ctx, task.Task, result)
	} else {
		err = w.applyPayment(ctx, task.Task, result)
	}
	if err != nil {
		// The lease expires and the attempt is retried; Payment deduplicates by operation_id.
		w.log.Error("record task attempt", "task_id", task.ID, "order_id", task.OrderID, "kind", kind, "error", err)
	}
}

type startAttempt struct {
	hold   domain.HoldParams
	detail string
}

// prepareStart rechecks START after dispatch and reads the deposit from the locked order.
// The transaction ends before calling Payment.
func (w *Worker) prepareStart(ctx context.Context, claimed domain.Task) (*startAttempt, error) {
	var start *startAttempt
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		start = nil
		order, task, _, owned, err := lockOwnedStart(ctx, q, claimed)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil || !owned {
			return err
		}

		start = &startAttempt{}
		if err := json.Unmarshal(task.Params, &start.hold); err != nil {
			start.detail = "invalid task parameters: " + err.Error()
			return nil
		}
		storedAmount := start.hold.Amount
		start.hold.Amount = order.Terms.Deposit
		if storedAmount == start.hold.Amount {
			return nil
		}
		// Earlier claims may have sent a different amount for this operation_id.
		if task.AttemptCount > 1 {
			start.detail = fmt.Sprintf("HOLD amount %d differs from order deposit %d after a prior attempt; reconcile in Payment before retrying",
				storedAmount, start.hold.Amount)
			return nil
		}
		// Persist the correction before the first Payment call so retries reuse it.
		return q.UpdateStartHoldParams(ctx, task.ID, start.hold)
	})
	return start, err
}

// call performs the Payment command of an attempt. START without a deposit needs no HOLD.
func (w *Worker) call(ctx context.Context, task domain.Task, start *startAttempt) payment.Result {
	invalid := func(err error) payment.Result {
		return payment.Result{Outcome: payment.ContractError, Detail: "invalid task parameters: " + err.Error()}
	}
	switch task.Kind {
	case domain.TaskStart:
		if start.detail != "" {
			return payment.Result{Outcome: payment.ContractError, Detail: start.detail}
		}
		if start.hold.Amount == 0 {
			return payment.Result{Outcome: payment.Success}
		}
		return w.payments.Hold(ctx, start.hold)
	case domain.TaskSettle:
		var p domain.SettleParams
		if err := json.Unmarshal(task.Params, &p); err != nil {
			return invalid(err)
		}
		return w.payments.Settle(ctx, p)
	case domain.TaskVoid:
		var p domain.VoidParams
		if err := json.Unmarshal(task.Params, &p); err != nil {
			return invalid(err)
		}
		return w.payments.Void(ctx, p)
	default:
		return invalid(fmt.Errorf("unknown task kind %q", task.Kind))
	}
}

// applyStart records a START attempt: ACTIVE on success, START_FAILED on a final
// failure, a retry on a transient error.
func (w *Worker) applyStart(ctx context.Context, claimed domain.Task, result payment.Result) error {
	var event *taskEvent
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		event = nil
		order, task, now, owned, err := lockOwnedStart(ctx, q, claimed)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		if !owned {
			metrics.StaleClaims.WithLabelValues(string(claimed.Kind)).Inc()
			return nil
		}
		switch result.Outcome {
		case payment.Success:
			if err := q.MarkOrderActive(ctx, order.ID, now); err != nil {
				return err
			}
			event, err = completeTask(ctx, q, task, now, domain.ResultSuccess, nil, "")
			if err != nil {
				return err
			}
			metrics.OrderActivation.Observe(now.Sub(order.CreatedAt).Seconds())
		case payment.Rejected:
			event, err = w.failStart(ctx, q, order, task, now, domain.ReasonPaymentRejected, false, "")
		case payment.ContractError:
			event, err = w.failStart(ctx, q, order, task, now, domain.ReasonContractError, true, result.Detail)
		default:
			return w.reschedule(ctx, q, task, now, result.Detail)
		}
		return err
	})
	if err == nil {
		w.report(event)
	}
	return err
}

// lockOwnedStart locks the order and then the START task and checks that the claim
// still owns the attempt and the order still waits for it.
func lockOwnedStart(ctx context.Context, q *storage.Queries, claimed domain.Task) (domain.Order, domain.Task, time.Time, bool, error) {
	order, err := q.LockOrder(ctx, claimed.OrderID)
	if err != nil {
		return domain.Order{}, domain.Task{}, time.Time{}, false, err
	}
	task, err := q.LockTask(ctx, claimed.ID)
	if err != nil {
		return domain.Order{}, domain.Task{}, time.Time{}, false, err
	}
	now, err := q.Now(ctx)
	if err != nil {
		return domain.Order{}, domain.Task{}, time.Time{}, false, err
	}
	owned := claimed.ClaimID != nil && task.Owns(*claimed.ClaimID, now) && order.Status == domain.OrderStarting
	return order, task, now, owned, nil
}

// applyPayment records a SETTLE or VOID attempt.
func (w *Worker) applyPayment(ctx context.Context, claimed domain.Task, result payment.Result) error {
	var event *taskEvent
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		event = nil
		task, err := q.LockTask(ctx, claimed.ID)
		if err != nil {
			return err
		}
		now, err := q.Now(ctx)
		if err != nil {
			return err
		}
		if !task.Owns(*claimed.ClaimID, now) {
			metrics.StaleClaims.WithLabelValues(string(claimed.Kind)).Inc()
			return nil
		}
		switch {
		case result.Outcome == payment.Success:
			event, err = completeTask(ctx, q, task, now, domain.ResultSuccess, nil, "")
		case result.Outcome == payment.Rejected && task.Kind == domain.TaskSettle:
			// The payment cycle ended with a debt in Payment.
			event, err = completeTask(ctx, q, task, now, domain.ResultFailed, nil, "")
		case result.Outcome == payment.Transient:
			return w.reschedule(ctx, q, task, now, result.Detail)
		default:
			// An unexpected 4xx, or "failed" for VOID, needs manual analysis.
			detail := result.Detail
			if detail == "" {
				detail = "unexpected result " + result.Outcome.String()
			}
			var lastError string
			lastError, err = q.DeadLetterTask(ctx, task.ID, detail)
			event = &taskEvent{task: task, status: domain.TaskDeadLetter, lastError: lastError}
		}
		return err
	})
	if err == nil {
		w.report(event)
	}
	return err
}

// failStart closes a START that will not succeed. The order becomes START_FAILED
// unless the API already closed it; compensate creates a VOID for a positive deposit.
func (w *Worker) failStart(ctx context.Context, q *storage.Queries, order domain.Order, task domain.Task, now time.Time,
	reason domain.Reason, compensate bool, lastError string) (*taskEvent, error) {
	if order.Status == domain.OrderStarting {
		deleteAfter := w.configs.Current().Retention.DeleteAfter(now, order.ZoneID)
		if err := q.MarkOrderStartFailed(ctx, order.ID, now, deleteAfter); err != nil {
			return nil, err
		}
	}
	if compensate && order.Terms.Deposit > 0 {
		void := domain.VoidParams{OperationID: order.ID.String()}
		if err := q.InsertTask(ctx, order.ID, domain.TaskVoid, void, now, nil); err != nil {
			return nil, err
		}
	}
	return completeTask(ctx, q, task, now, domain.ResultFailed, &reason, lastError)
}

func completeTask(ctx context.Context, q *storage.Queries, task domain.Task, now time.Time,
	result string, reason *domain.Reason, detail string) (*taskEvent, error) {
	lastError, err := q.CompleteTask(ctx, task.ID, now, result, reason, detail)
	if err != nil {
		return nil, err
	}
	event := &taskEvent{task: task, status: domain.TaskCompleted, result: result, lastError: lastError}
	if reason != nil {
		event.reason = *reason
	}
	return event, nil
}

// reschedule releases the claim after a transient failure. A START retry is never
// scheduled past its deadline; the sweep closes it there.
func (w *Worker) reschedule(ctx context.Context, q *storage.Queries, task domain.Task, now time.Time, detail string) error {
	next := now.Add(domain.Backoff(task.AttemptCount, w.cfg.BackoffBase, w.cfg.BackoffMax, w.jitter))
	if task.Deadline != nil && next.After(*task.Deadline) {
		next = *task.Deadline
	}
	return q.RescheduleTask(ctx, task.ID, next, detail)
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
