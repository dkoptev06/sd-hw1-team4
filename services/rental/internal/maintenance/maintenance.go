// Package maintenance deletes completed tasks, archives closed orders into weekly
// orders_history partitions, deletes expired offers and drops expired partitions.
// Replicas coordinate through a lease in PostgreSQL; only the holder runs a cycle.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

const leaseName = "maintenance"

type Maintenance struct {
	store  *storage.Store
	cfg    settings.Maintenance
	holder uuid.UUID
	log    *slog.Logger
	now    func() time.Time
}

func New(store *storage.Store, cfg settings.Maintenance, log *slog.Logger) *Maintenance {
	return &Maintenance{store: store, cfg: cfg, holder: uuid.New(), log: log, now: time.Now}
}

// Run executes a cycle every interval while holding the lease, until ctx is done.
func (m *Maintenance) Run(ctx context.Context) error {
	go m.statsLoop(ctx)
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		held, err := m.renewLease(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			m.log.Error("acquire maintenance lease", "error", err)
		case held:
			if err := m.RunCycle(ctx); err != nil && ctx.Err() == nil {
				m.log.Error("maintenance cycle", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			m.release()
			return nil
		case <-ticker.C:
		}
	}
}

func (m *Maintenance) renewLease(ctx context.Context) (bool, error) {
	held, err := m.store.AcquireLease(ctx, leaseName, m.holder, m.cfg.Lease)
	if held {
		metrics.MaintenanceLeaseHeld.Set(1)
	} else {
		metrics.MaintenanceLeaseHeld.Set(0)
	}
	return held, err
}

func (m *Maintenance) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.ReleaseLease(ctx, leaseName, m.holder); err != nil {
		m.log.Warn("release maintenance lease", "error", err)
	}
	metrics.MaintenanceLeaseHeld.Set(0)
}

// errLeaseLost stops a cycle when another replica took the lease.
var errLeaseLost = errors.New("maintenance lease lost")

// RunCycle runs all steps once. Each step works in batches until it runs out of work.
// The order matters: tasks are deleted before orders are archived, because an order
// is archived only when it has no tasks.
func (m *Maintenance) RunCycle(ctx context.Context) error {
	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{"purge_tasks", m.purgeCompletedTasks},
		{"ensure_partitions", m.ensureUpcomingPartitions},
		{"archive_orders", m.archiveOrders},
		{"purge_offers", m.purgeExpiredOffers},
		{"drop_partitions", m.dropExpiredPartitions},
	}
	for _, step := range steps {
		started := time.Now()
		err := step.run(ctx)
		metrics.MaintenanceStepDuration.WithLabelValues(step.name).Observe(time.Since(started).Seconds())
		if err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return nil
}

// batches calls fn until it processes less than a full batch, renewing the lease in between.
func (m *Maintenance) batches(ctx context.Context, step string, fn func() (int64, error)) error {
	for {
		n, err := fn()
		if err != nil {
			return err
		}
		metrics.MaintenanceRows.WithLabelValues(step).Add(float64(n))
		if n < int64(m.cfg.BatchSize) {
			return nil
		}
		held, err := m.renewLease(ctx)
		if err != nil {
			return err
		}
		if !held {
			return errLeaseLost
		}
	}
}

func (m *Maintenance) purgeCompletedTasks(ctx context.Context) error {
	return m.batches(ctx, "purge_tasks", func() (int64, error) {
		return m.store.PurgeCompletedTasks(ctx, m.cfg.BatchSize)
	})
}

func (m *Maintenance) archiveOrders(ctx context.Context) error {
	var archived int64
	err := m.batches(ctx, "archive_orders", func() (int64, error) {
		removed, moved, err := m.archiveBatch(ctx)
		archived += moved
		return removed, err
	})
	metrics.MaintenanceRows.WithLabelValues("archive_orders_moved").Add(float64(archived))
	return err
}

// archiveBatch creates the history partitions the next batch needs and archives it.
// If the batch differs from the inspected candidates (rows locked or added in
// between) and needs a missing partition, it inspects the candidates again.
func (m *Maintenance) archiveBatch(ctx context.Context) (removed, archived int64, err error) {
	const maxAttempts = 4
	for attempt := 1; ; attempt++ {
		weeks, err := m.store.ArchiveCandidateWeeks(ctx, m.cfg.BatchSize)
		if err != nil {
			return 0, 0, err
		}
		if err := m.ensurePartitions(ctx, weeks); err != nil {
			return 0, 0, err
		}
		removed, archived, err = m.store.ArchiveOrders(ctx, m.cfg.BatchSize)
		if !storage.IsMissingPartition(err) || attempt == maxAttempts {
			return removed, archived, err
		}
	}
}

func (m *Maintenance) purgeExpiredOffers(ctx context.Context) error {
	return m.batches(ctx, "purge_offers", func() (int64, error) {
		return m.store.PurgeExpiredOffers(ctx, m.cfg.OfferGrace, m.cfg.BatchSize)
	})
}
