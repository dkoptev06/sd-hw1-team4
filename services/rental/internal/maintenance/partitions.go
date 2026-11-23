package maintenance

import (
	"context"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

// ensureUpcomingPartitions keeps partitions ready for the default retention horizon,
// so archiving rarely has to create one. Longer zone retention is covered on demand.
func (m *Maintenance) ensureUpcomingPartitions(ctx context.Context) error {
	first := storage.WeekStart(m.now())
	weeks := make([]time.Time, 0, m.cfg.WeeksAhead)
	for i := range m.cfg.WeeksAhead {
		weeks = append(weeks, first.AddDate(0, 0, 7*i))
	}
	return m.ensurePartitions(ctx, weeks)
}

// ensurePartitions creates the missing partitions of the given weeks.
func (m *Maintenance) ensurePartitions(ctx context.Context, weeks []time.Time) error {
	if len(weeks) == 0 {
		return nil
	}
	existing, err := m.store.ListPartitions(ctx)
	if err != nil {
		return err
	}
	have := make(map[string]bool, len(existing))
	for _, p := range existing {
		have[p.Name] = true
	}
	for _, week := range weeks {
		name := storage.PartitionName(storage.WeekStart(week))
		if have[name] {
			continue
		}
		if err := m.store.CreatePartition(ctx, week); err != nil {
			return err
		}
		have[name] = true
		m.log.Info("created history partition", "partition", name)
	}
	return nil
}

// dropExpiredPartitions removes partitions whose whole range is past delete_after.
// It first completes a cleanup interrupted by a crash: a pending concurrent detach is
// finalized and detached tables are dropped.
func (m *Maintenance) dropExpiredPartitions(ctx context.Context) error {
	partitions, err := m.store.ListPartitions(ctx)
	if err != nil {
		return err
	}
	cutoff := m.now()
	attached := 0
	for _, p := range partitions {
		if p.DetachPending {
			m.log.Warn("finalizing interrupted partition detach", "partition", p.Name)
			if err := m.store.FinalizeDetach(ctx, p.Name); err != nil {
				return err
			}
			continue
		}
		week, ok := storage.ParsePartitionName(p.Name)
		if !ok || week.AddDate(0, 0, 7).After(cutoff) {
			attached++
			continue
		}
		if err := m.store.DetachPartition(ctx, p.Name); err != nil {
			return err
		}
		m.log.Info("detached expired history partition", "partition", p.Name)
	}
	metrics.HistoryPartitions.Set(float64(attached))

	detached, err := m.store.ListDetachedPartitions(ctx)
	if err != nil {
		return err
	}
	for _, name := range detached {
		if err := m.store.DropTable(ctx, name); err != nil {
			return err
		}
		metrics.MaintenanceRows.WithLabelValues("drop_partitions").Inc()
		m.log.Info("dropped history partition", "partition", name)
	}
	return nil
}
