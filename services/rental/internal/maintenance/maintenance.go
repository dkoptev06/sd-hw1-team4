// Package maintenance deletes completed tasks, archives closed orders into weekly
// orders_history partitions, deletes expired offers and drops expired partitions.
// Replicas coordinate through a PostgreSQL lease and a session lock during each cycle.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

const leaseName = "maintenance"

// Options configures archive batches, history cleanup, and the maintenance lease.
type Options struct {
	Interval      time.Duration
	Lease         time.Duration
	BatchSize     int
	OfferGrace    time.Duration
	WeeksAhead    int
	StatsInterval time.Duration
}

type Service struct {
	pool   *pgxpool.Pool
	tasks  *task.Service
	cfg    Options
	holder uuid.UUID
	log    *slog.Logger
}

func NewService(pool *pgxpool.Pool, tasks *task.Service, cfg Options, log *slog.Logger) *Service {
	return &Service{pool: pool, tasks: tasks, cfg: cfg, holder: uuid.New(), log: log}
}

// Run executes a cycle every interval while holding the lease, until ctx is done.
func (m *Service) Run(ctx context.Context) error {
	go m.statsLoop(ctx)
	// Long-running partition DDL uses a separate session. Coalesce requests
	// while old readers hold a DETACH open and keep draining the order queue.
	requests := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range requests {
			if err := m.cleanPartitions(ctx); err != nil && ctx.Err() == nil {
				m.log.Error("partition cleanup", "error", err)
			}
		}
	}()
	defer func() {
		close(requests)
		<-done
		m.release()
	}()
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		held, err := m.archiveCycle(ctx)
		if err != nil && ctx.Err() == nil {
			m.log.Error("maintenance cycle", "error", err)
		}
		if held {
			select {
			case requests <- struct{}{}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (m *Service) renewLease(ctx context.Context, q repository) (bool, error) {
	held, err := q.acquireLease(ctx, leaseName, m.holder, m.cfg.Lease)
	if held {
		metrics.MaintenanceLeaseHeld.Set(1)
	} else {
		metrics.MaintenanceLeaseHeld.Set(0)
	}
	return held, err
}

func (m *Service) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := (repository{db: m.pool}).releaseLease(ctx, leaseName, m.holder); err != nil {
		m.log.Warn("release maintenance lease", "error", err)
	}
	metrics.MaintenanceLeaseHeld.Set(0)
}

// errLeaseLost stops a cycle when another replica took the lease.
var errLeaseLost = errors.New("maintenance lease lost")

func (m *Service) archiveCycle(ctx context.Context) (bool, error) {
	var held bool
	locked, err := withSessionLock(ctx, m.pool, leaseName, func(q repository) error {
		var err error
		held, err = m.renewLease(ctx, q)
		if !held || err != nil {
			return err
		}
		return m.runCycle(ctx, q)
	})
	if !locked {
		metrics.MaintenanceLeaseHeld.Set(0)
	}
	return held, err
}

// runCycle deletes completed tasks before selecting task-free orders for archiving.
func (m *Service) runCycle(ctx context.Context, q repository) error {
	steps := []struct {
		name string
		run  func(context.Context, repository) error
	}{
		{"schedule_partitions", m.scheduleExpiredPartitions},
		{"purge_tasks", m.purgeCompletedTasks},
		{"ensure_partitions", m.ensureUpcomingPartitions},
		{"archive_orders", m.archiveOrders},
		{"purge_offers", m.purgeExpiredOffers},
	}
	for _, step := range steps {
		started := time.Now()
		err := step.run(ctx, q)
		metrics.MaintenanceStepDuration.WithLabelValues(step.name).Observe(time.Since(started).Seconds())
		if err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return nil
}

// batches calls fn until it processes less than a full batch, renewing the lease in between.
func (m *Service) batches(ctx context.Context, q repository, step string, fn func() (int64, error)) error {
	for {
		n, err := fn()
		if err != nil {
			return err
		}
		metrics.MaintenanceRows.WithLabelValues(step).Add(float64(n))
		if n < int64(m.cfg.BatchSize) {
			return nil
		}
		held, err := m.renewLease(ctx, q)
		if err != nil {
			return err
		}
		if !held {
			return errLeaseLost
		}
	}
}

func (m *Service) purgeCompletedTasks(ctx context.Context, q repository) error {
	return m.batches(ctx, q, "purge_tasks", func() (int64, error) {
		return q.purgeCompletedTasks(ctx, m.cfg.BatchSize)
	})
}

func (m *Service) archiveOrders(ctx context.Context, q repository) error {
	var archived int64
	err := m.batches(ctx, q, "archive_orders", func() (int64, error) {
		removed, moved, err := m.archiveBatch(ctx, q)
		archived += moved
		return removed, err
	})
	metrics.MaintenanceRows.WithLabelValues("archive_orders_moved").Add(float64(archived))
	return err
}

// archiveBatch creates the history partitions the next batch needs and archives it.
// If the batch differs from the inspected candidates (rows locked or added in
// between) and needs a missing partition, it inspects the candidates again.
func (m *Service) archiveBatch(ctx context.Context, q repository) (removed, archived int64, err error) {
	const maxAttempts = 4
	for attempt := 1; ; attempt++ {
		weeks, err := q.archiveCandidateWeeks(ctx, m.cfg.BatchSize)
		if err != nil {
			return 0, 0, err
		}
		readyWeeks, err := m.ensurePartitions(ctx, q, weeks)
		if err != nil {
			return 0, 0, err
		}
		removed, archived, err = q.archiveOrders(ctx, m.cfg.BatchSize, readyWeeks)
		if !isMissingPartition(err) || attempt == maxAttempts {
			return removed, archived, err
		}
	}
}

func (m *Service) purgeExpiredOffers(ctx context.Context, q repository) error {
	return m.batches(ctx, q, "purge_offers", func() (int64, error) {
		return q.purgeExpiredOffers(ctx, m.cfg.OfferGrace, m.cfg.BatchSize)
	})
}
