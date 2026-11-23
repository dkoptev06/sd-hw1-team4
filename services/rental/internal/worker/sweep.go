package worker

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

func (w *Worker) sweepLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.Sweep(ctx); err != nil && ctx.Err() == nil {
			w.log.Error("sweep tasks", "error", err)
		}
		sleep(ctx, w.cfg.SweepInterval)
	}
}

// Sweep closes START tasks past their deadline and tasks without attempts left. It
// repeats batches while they come back full and make progress, so it keeps up with
// a backlog (for example, every START expiring during a Payment outage).
func (w *Worker) Sweep(ctx context.Context) error {
	for ctx.Err() == nil {
		more, err := w.sweepBatch(ctx)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

// sweepBatch handles one batch of each kind and reports whether another batch may
// have work. Rows locked by another replica or by an attempt being applied are
// skipped, so replicas split the backlog instead of waiting on the same rows.
func (w *Worker) sweepBatch(ctx context.Context) (more bool, err error) {
	expired, err := w.store.FindStartsPastDeadline(ctx, w.cfg.BatchSize)
	if err != nil {
		return false, err
	}
	var errs []error
	closed := 0
	record := func(done bool, err error) {
		if done {
			closed++
		}
		errs = append(errs, err)
	}
	for _, orderID := range expired {
		record(w.finalizeStart(ctx, orderID, skipLocked))
	}
	exhausted, err := w.store.FindExhaustedTasks(ctx, w.cfg.BatchSize, w.maxAttempts)
	if err != nil {
		return false, errors.Join(append(errs, err)...)
	}
	for _, ref := range exhausted {
		if ref.Kind == domain.TaskStart {
			record(w.finalizeStart(ctx, ref.OrderID, skipLocked))
		} else {
			record(w.deadLetterExhausted(ctx, ref.ID))
		}
	}
	full := len(expired) == w.cfg.BatchSize || len(exhausted) == w.cfg.BatchSize
	return full && closed > 0, errors.Join(errs...)
}

// lockMode tells whether a sweep waits for rows locked by other transactions.
type lockMode bool

const (
	waitLocked lockMode = false
	skipLocked lockMode = true
)

// FinalizeStart closes a pending START without a new HOLD when its deadline passed
// or its attempts ran out. It is a no-op when neither holds or another participant
// already closed the task.
func (w *Worker) FinalizeStart(ctx context.Context, orderID uuid.UUID) error {
	_, err := w.finalizeStart(ctx, orderID, waitLocked)
	return err
}

// finalizeStart reports whether it closed the task. With skipLocked a locked order
// counts as nothing to do.
func (w *Worker) finalizeStart(ctx context.Context, orderID uuid.UUID, mode lockMode) (bool, error) {
	var event *taskEvent
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		event = nil
		lockOrder := q.LockOrder
		if mode == skipLocked {
			lockOrder = q.TryLockOrder
		}
		order, err := lockOrder(ctx, orderID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		task, err := q.LockStartTask(ctx, orderID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		now, err := q.Now(ctx)
		if err != nil {
			return err
		}
		reason := domain.StartFailureReason(task, order.Status, now, w.maxAttempts[domain.TaskStart])
		if reason == "" {
			return nil
		}
		if order.Status != domain.OrderStarting && order.Status != domain.OrderStartFailed {
			w.log.Error("pending START for an order that is not starting", "order_id", orderID, "status", order.Status)
			return nil
		}
		event, err = w.failStart(ctx, q, order, task, now, reason, true, "")
		return err
	})
	if err != nil {
		return false, err
	}
	w.report(event)
	return event != nil, nil
}

// deadLetterExhausted moves a SETTLE or VOID without attempts left to DEAD_LETTER
// and reports whether it did. A task locked by another transaction is skipped.
func (w *Worker) deadLetterExhausted(ctx context.Context, taskID uuid.UUID) (bool, error) {
	var event *taskEvent
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		event = nil
		task, err := q.TryLockTask(ctx, taskID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		now, err := q.Now(ctx)
		if err != nil {
			return err
		}
		if !task.Exhausted(now, w.maxAttempts[task.Kind]) {
			return nil
		}
		event = &taskEvent{task: task, status: domain.TaskDeadLetter, lastError: "max attempts exceeded"}
		return q.DeadLetterTask(ctx, task.ID, "")
	})
	if err != nil {
		return false, err
	}
	w.report(event)
	return event != nil, nil
}

// taskEvent describes a task that reached COMPLETED or DEAD_LETTER.
type taskEvent struct {
	task      domain.Task
	status    domain.TaskStatus
	result    string
	reason    domain.Reason
	lastError string
}

// report logs a committed task completion and updates metrics. The log is the
// record of the task after maintenance deletes it.
func (w *Worker) report(e *taskEvent) {
	if e == nil {
		return
	}
	metrics.TasksFinished.WithLabelValues(string(e.task.Kind), string(e.status), string(e.reason)).Inc()
	attrs := []any{
		"task_id", e.task.ID, "order_id", e.task.OrderID, "kind", e.task.Kind, "status", e.status,
		"result", e.result, "attempts", e.task.AttemptCount, "completion_reason", e.reason, "last_error", e.lastError,
	}
	if e.status == domain.TaskDeadLetter {
		w.log.Error("task moved to dead letter queue", attrs...)
		return
	}
	w.log.Info("task finished", attrs...)
}
