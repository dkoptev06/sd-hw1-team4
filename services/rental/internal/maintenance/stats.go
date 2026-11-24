package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

// statsLoop exports database state gauges: queue sizes, DLQ, archiving lag, DB size.
// Every replica exports them; dashboards aggregate with max().
func (m *Service) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.StatsInterval)
	defer ticker.Stop()
	for {
		if err := m.exportStats(ctx); err != nil && ctx.Err() == nil {
			m.log.Warn("collect database stats", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Service) exportStats(ctx context.Context) error {
	stats, err := (repository{db: m.pool}).stats(ctx)
	if err != nil {
		return err
	}
	metrics.TasksByState.Reset()
	for _, kind := range []task.Kind{task.Start, task.Settle, task.Void} {
		for _, status := range []task.Status{task.Pending, task.Completed, task.DeadLetter} {
			metrics.TasksByState.WithLabelValues(string(kind), string(status)).Set(0)
		}
	}
	for _, t := range stats.Tasks {
		metrics.TasksByState.WithLabelValues(t.Kind, t.Status).Set(float64(t.Count))
	}
	metrics.OldestTaskAge.Reset()
	for status, age := range stats.OldestTaskAge {
		metrics.OldestTaskAge.WithLabelValues(status).Set(age)
	}
	for _, status := range []order.Status{order.Starting, order.Active, order.StartFailed,
		order.Completed, order.Cancelled} {
		metrics.OrdersByStatus.WithLabelValues(string(status)).Set(float64(stats.Orders[string(status)]))
	}
	metrics.OldestClosedOrderAge.Set(stats.OldestClosedOrderAge)
	metrics.ArchiveOldestReadyOrderAge.Set(stats.OldestReadyOrderAge)
	metrics.DatabaseBytes.Set(float64(stats.DatabaseBytes))
	ready, err := m.tasks.ReadyStats(ctx)
	if err != nil {
		return fmt.Errorf("collect ready task stats: %w", err)
	}
	for _, kind := range []task.Kind{task.Start, task.Settle, task.Void} {
		stats := ready[kind]
		metrics.ReadyTasks.WithLabelValues(string(kind)).Set(float64(stats.Count))
		metrics.OldestReadyTaskWait.WithLabelValues(string(kind)).Set(stats.OldestWaitSeconds)
	}
	return nil
}
