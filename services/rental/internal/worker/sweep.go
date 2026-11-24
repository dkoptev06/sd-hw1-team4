package worker

import (
	"context"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func (w *Worker) sweepLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := w.Sweep(ctx); err != nil && ctx.Err() == nil {
			w.log.Error("sweep tasks", "error", err)
		}
		sleep(ctx, w.cfg.SweepInterval)
	}
}

// Sweep closes START and moves exhausted payment tasks to the DLQ.
func (w *Worker) Sweep(ctx context.Context) error {
	return w.tasks.Sweep(ctx, w.cfg.BatchSize, func(candidate task.SweepTask) error {
		var event *task.Event
		var err error
		if candidate.Kind == task.Start {
			event, err = w.orders.FinalizeStart(ctx, candidate.OrderID, &candidate.ID)
		} else {
			event, err = w.tasks.DeadLetterExhausted(ctx, candidate.ID)
		}
		if err == nil {
			w.report(event)
		}
		return err
	})
}

// report logs a committed task completion and updates metrics. The log is the
// record of the task after maintenance deletes it.
func (w *Worker) report(e *task.Event) {
	if e == nil {
		return
	}
	metrics.TasksFinished.WithLabelValues(string(e.Kind), string(e.Status), string(e.Reason)).Inc()
	attrs := []any{
		"task_id", e.ID, "order_id", e.OrderID, "kind", e.Kind, "status", e.Status,
		"result", e.Result, "attempts", e.Attempts, "completion_reason", e.Reason, "last_error", e.LastError,
	}
	if e.Status == task.DeadLetter {
		if e.Cause != "" {
			attrs = append(attrs, "cause", e.Cause)
		}
		w.log.Error("task moved to dead letter queue", attrs...)
		return
	}
	w.log.Info("task finished", attrs...)
}
