package maintenance

import (
	"context"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
)

// statsLoop exports database state gauges: queue sizes, DLQ, archiving lag, DB size.
// Every replica exports them; dashboards aggregate with max().
func (m *Maintenance) statsLoop(ctx context.Context) {
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

func (m *Maintenance) exportStats(ctx context.Context) error {
	stats, err := m.store.Stats(ctx)
	if err != nil {
		return err
	}
	metrics.TasksByState.Reset()
	for _, kind := range []domain.TaskKind{domain.TaskStart, domain.TaskSettle, domain.TaskVoid} {
		for _, status := range []domain.TaskStatus{domain.TaskPending, domain.TaskCompleted, domain.TaskDeadLetter} {
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
	for _, status := range []domain.OrderStatus{domain.OrderStarting, domain.OrderActive, domain.OrderStartFailed,
		domain.OrderCompleted, domain.OrderCancelled} {
		metrics.OrdersByStatus.WithLabelValues(string(status)).Set(float64(stats.Orders[string(status)]))
	}
	metrics.OldestClosedOrderAge.Set(stats.OldestClosedOrderAge)
	metrics.DatabaseBytes.Set(float64(stats.DatabaseBytes))
	return nil
}
