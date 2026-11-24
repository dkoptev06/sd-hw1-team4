package maintenance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

// History partitions cover one week each, from Monday 00:00 UTC, and are named orders_history_pYYYYMMDD.
const partitionPrefix = "orders_history_p"

// weekStart returns the Monday 00:00 UTC of the week containing t.
func weekStart(t time.Time) time.Time {
	day := t.UTC().Truncate(24 * time.Hour)
	return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
}

func partitionName(week time.Time) string {
	return partitionPrefix + week.Format("20060102")
}

// parsePartitionName returns the week of a partition created by createPartition.
func parsePartitionName(name string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(name, partitionPrefix)
	if !ok {
		return time.Time{}, false
	}
	date, suffix, hasSuffix := strings.Cut(rest, "_")
	if hasSuffix {
		if _, err := uuid.Parse(suffix); err != nil || len(suffix) != 36 {
			return time.Time{}, false
		}
	}
	week, err := time.Parse("20060102", date)
	return week, err == nil && week.Equal(weekStart(week))
}

// ensureUpcomingPartitions keeps partitions ready for the default retention horizon,
// so archiving rarely has to create one. Longer zone retention is covered on demand.
func (m *Service) ensureUpcomingPartitions(ctx context.Context, q repository) error {
	now, err := postgres.Now(ctx, q.db)
	if err != nil {
		return err
	}
	first := weekStart(now)
	weeks := make([]time.Time, 0, m.cfg.WeeksAhead)
	for i := range m.cfg.WeeksAhead {
		weeks = append(weeks, first.AddDate(0, 0, 7*i))
	}
	_, err = m.ensurePartitions(ctx, q, weeks)
	return err
}

// ensurePartitions creates the missing partitions of the given weeks.
func (m *Service) ensurePartitions(ctx context.Context, q repository, weeks []time.Time) ([]time.Time, error) {
	existing, err := q.listPartitions(ctx)
	if err != nil {
		return nil, err
	}
	ready := make(map[time.Time]bool, len(existing))
	for _, p := range existing {
		if week, ok := parsePartitionName(p.Name); ok && !p.DetachPending && !p.Scheduled {
			ready[week] = true
		}
	}
	for _, week := range weeks {
		if ready[week] {
			continue
		}
		scheduled, err := q.partitionDropScheduled(ctx, week)
		if err != nil {
			return nil, err
		}
		if scheduled {
			continue
		}
		created, err := q.tryCreatePartition(ctx, week)
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
func (m *Service) scheduleExpiredPartitions(ctx context.Context, q repository) error {
	partitions, err := q.listPartitions(ctx)
	if err != nil {
		return err
	}
	cutoff, err := postgres.Now(ctx, q.db)
	if err != nil {
		return err
	}
	metrics.HistoryPartitions.Set(float64(len(partitions)))
	for _, p := range partitions {
		week, ok := parsePartitionName(p.Name)
		if !ok || p.Scheduled || (!p.DetachPending && week.After(cutoff)) {
			continue
		}
		if !p.DetachPending && week.AddDate(0, 0, 7).After(cutoff) {
			latest, err := q.latestPartitionExpiry(ctx, p.Name)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			if latest.After(cutoff) {
				continue
			}
		}
		if err := q.schedulePartitionDrop(ctx, p.Name, week); err != nil {
			return err
		}
	}
	return nil
}

// A session lock fences an interrupted DETACH from other cleanup replicas. The
// archive lock stays available while PostgreSQL is waiting for existing readers.
func (m *Service) cleanPartitions(ctx context.Context) error {
	_, err := withSessionLock(ctx, m.pool, "history_partition_cleanup", func(q repository) error {
		started := time.Now()
		defer func() {
			metrics.MaintenanceStepDuration.WithLabelValues("drop_partitions").Observe(time.Since(started).Seconds())
		}()
		drops, err := q.pendingPartitionDrops(ctx)
		if err != nil || len(drops) == 0 {
			return err
		}
		if m.pool.Config().MaxConns == 1 {
			return q.withLockTimeout(ctx, 100*time.Millisecond, func() error { return m.applyDrops(ctx, q, drops) })
		}
		return m.applyDrops(ctx, q, drops)
	})
	return err
}

func (m *Service) applyDrops(ctx context.Context, q repository, drops []partitionDropRow) error {
	for _, p := range drops {
		var err error
		if p.Pending {
			err = q.finalizeDetach(ctx, p.Name)
		} else if p.Attached {
			err = q.detachPartition(ctx, p.Name)
		}
		if err != nil {
			return fmt.Errorf("detach %s: %w", p.Name, err)
		}
		// Keep locator WAL and lock time bounded per transaction and per cycle.
		for range 8 {
			dropped, more, err := q.finishPartitionDrop(ctx, p, m.cfg.BatchSize)
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
