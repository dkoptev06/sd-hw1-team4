package worker

import (
	"context"
	"errors"
	"fmt"

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

// Sweep walks both queues in bounded pages. A fixed database cutoff keeps
// incoming tasks from extending this pass indefinitely. Each pass advances past
// locked or failed rows and the next pass revisits them from the beginning.
func (w *Worker) Sweep(ctx context.Context) error {
	cutoff, err := w.store.Now(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	process := func(tasks []storage.SweepTask) {
		for _, task := range tasks {
			var err error
			if task.Kind == domain.TaskStart {
				err = w.finalizeStart(ctx, task.OrderID, &task.ID)
			} else {
				err = w.deadLetterExhausted(ctx, task.ID)
			}
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("finalize %s task %s: %w", task.Kind, task.ID, err)
			}
		}
	}

	var expiredAfter, exhaustedAfter storage.SweepTask
	for ctx.Err() == nil {
		expired, err := w.store.FindStartsPastDeadline(ctx, w.cfg.BatchSize, cutoff, expiredAfter)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		process(expired)
		exhausted, err := w.store.FindExhaustedTasks(ctx, w.cfg.BatchSize, w.maxAttempts, cutoff, exhaustedAfter)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		process(exhausted)
		if len(expired) < w.cfg.BatchSize && len(exhausted) < w.cfg.BatchSize {
			return firstErr
		}
		if len(expired) > 0 {
			expiredAfter = expired[len(expired)-1]
		}
		if len(exhausted) > 0 {
			exhaustedAfter = exhausted[len(exhausted)-1]
		}
	}
	return errors.Join(firstErr, ctx.Err())
}

// FinalizeStart closes a pending START without a new HOLD when its deadline passed
// or its attempts ran out. It is a no-op when neither holds or another participant
// already closed the task.
func (w *Worker) FinalizeStart(ctx context.Context, orderID uuid.UUID) error {
	return w.finalizeStart(ctx, orderID, nil)
}

// A sweep supplies the selected task ID and skips locked rows. Explicit
// finalization waits for the order and then its current START task.
func (w *Worker) finalizeStart(ctx context.Context, orderID uuid.UUID, sweepTaskID *uuid.UUID) error {
	var event *taskEvent
	err := w.store.InTx(ctx, func(q *storage.Queries) error {
		event = nil
		lockOrder := q.LockOrder
		if sweepTaskID != nil {
			lockOrder = q.TryLockOrder
		}
		order, err := lockOrder(ctx, orderID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var task domain.Task
		if sweepTaskID != nil {
			task, err = q.TryLockTask(ctx, *sweepTaskID)
		} else {
			task, err = q.LockStartTask(ctx, orderID)
		}
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
		return err
	}
	w.report(event)
	return nil
}

// deadLetterExhausted moves a SETTLE or VOID without attempts left to DEAD_LETTER
// unless the task is locked by another transaction.
func (w *Worker) deadLetterExhausted(ctx context.Context, taskID uuid.UUID) error {
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
		lastError, err := q.DeadLetterTask(ctx, task.ID, "")
		if err != nil {
			return err
		}
		event = &taskEvent{task: task, status: domain.TaskDeadLetter, lastError: lastError, cause: "max attempts exceeded"}
		return nil
	})
	if err != nil {
		return err
	}
	w.report(event)
	return nil
}

// taskEvent describes a task that reached COMPLETED or DEAD_LETTER.
type taskEvent struct {
	task      domain.Task
	status    domain.TaskStatus
	result    string
	reason    domain.Reason
	lastError string
	cause     string
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
		if e.cause != "" {
			attrs = append(attrs, "cause", e.cause)
		}
		w.log.Error("task moved to dead letter queue", attrs...)
		return
	}
	w.log.Info("task finished", attrs...)
}
