package maintenance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

// ensureUpcomingPartitions keeps partitions ready for the default retention horizon,
// so archiving rarely has to create one. Longer zone retention is covered on demand.
func (m *Maintenance) ensureUpcomingPartitions(ctx context.Context, q *storage.Queries) error {
	now, err := q.Now(ctx)
	if err != nil {
		return err
	}
	first := storage.WeekStart(now)
	weeks := make([]time.Time, 0, m.cfg.WeeksAhead)
	for i := range m.cfg.WeeksAhead {
		weeks = append(weeks, first.AddDate(0, 0, 7*i))
	}
	_, err = m.ensurePartitions(ctx, q, weeks)
	return err
}

// ensurePartitions creates the missing partitions of the given weeks.
func (m *Maintenance) ensurePartitions(ctx context.Context, q *storage.Queries, weeks []time.Time) ([]time.Time, error) {
	existing, err := q.ListPartitions(ctx)
	if err != nil {
		return nil, err
	}
	ready := make(map[time.Time]bool, len(existing))
	for _, p := range existing {
		if week, ok := storage.ParsePartitionName(p.Name); ok && !p.DetachPending && !p.Scheduled {
			ready[week] = true
		}
	}
	for _, week := range weeks {
		if ready[week] {
			continue
		}
		scheduled, err := q.PartitionDropScheduled(ctx, week)
		if err != nil {
			return nil, err
		}
		if scheduled {
			continue
		}
		created, err := q.TryCreatePartition(ctx, week)
		if err != nil {
			return nil, err
		}
		if !created {
			break
		}
		ready[week] = true
		m.log.Info("created history partition", "week", week)
	}
	result := make([]time.Time, 0, len(ready))
	for week := range ready {
		result = append(result, week)
	}
	return result, nil
}

// The archive session registers cleanup before releasing its lock. Subsequent
// batches skip the retiring week, so its last deadline cannot change during DDL.
func (m *Maintenance) scheduleExpiredPartitions(ctx context.Context, q *storage.Queries) error {
	partitions, err := q.ListPartitions(ctx)
	if err != nil {
		return err
	}
	cutoff, err := q.Now(ctx)
	if err != nil {
		return err
	}
	metrics.HistoryPartitions.Set(float64(len(partitions)))
	for _, p := range partitions {
		week, ok := storage.ParsePartitionName(p.Name)
		if !ok || p.Scheduled || (!p.DetachPending && week.After(cutoff)) {
			continue
		}
		if !p.DetachPending && week.AddDate(0, 0, 7).After(cutoff) {
			latest, err := q.LatestPartitionExpiry(ctx, p.Name)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if latest.After(cutoff) {
				continue
			}
		}
		if err := q.SchedulePartitionDrop(ctx, p.Name, week); err != nil {
			return err
		}
	}
	return nil
}

// A session lock fences an interrupted DETACH from other cleanup replicas. The
// archive lock stays available while PostgreSQL is waiting for existing readers.
func (m *Maintenance) cleanPartitions(ctx context.Context) error {
	_, err := m.store.WithMaintenanceLock(ctx, "history_partition_cleanup", func(q *storage.Queries) error {
		started := time.Now()
		defer func() {
			metrics.MaintenanceStepDuration.WithLabelValues("drop_partitions").Observe(time.Since(started).Seconds())
		}()
		drops, err := q.PendingPartitionDrops(ctx)
		if err != nil || len(drops) == 0 {
			return err
		}
		if m.store.Pool().Config().MaxConns == 1 {
			return q.WithLockTimeout(ctx, 100*time.Millisecond, func() error { return m.applyDrops(ctx, q, drops) })
		}
		return m.applyDrops(ctx, q, drops)
	})
	return err
}

func (m *Maintenance) applyDrops(ctx context.Context, q *storage.Queries, drops []storage.PartitionDrop) error {
	for _, p := range drops {
		var err error
		if p.Pending {
			err = q.FinalizeDetach(ctx, p.Name)
		} else if p.Attached {
			err = q.DetachPartition(ctx, p.Name)
		}
		if err != nil {
			return fmt.Errorf("detach %s: %w", p.Name, err)
		}
		// Keep locator WAL and lock time bounded per transaction and per cycle.
		for range 8 {
			dropped, more, err := q.FinishPartitionDrop(ctx, p, m.cfg.BatchSize)
			if err != nil {
				return err
			}
			if dropped {
				metrics.MaintenanceRows.WithLabelValues("drop_partitions").Inc()
				m.log.Info("dropped expired history partition", "partition", p.Name)
			}
			if !more {
				break
			}
		}
	}
	return nil
}
