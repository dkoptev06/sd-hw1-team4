package order

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

// PrepareStart reads the current deposit while holding both locks. Payment is called
// after this transaction commits with the parameters recorded for this attempt.
func (s *Service) PrepareStart(ctx context.Context, claimed task.Claim) (*task.PreparedHold, error) {
	var prepared *task.PreparedHold
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		o, locked, _, owned, err := s.lockOwnedStart(ctx, tx, claimed)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil || !owned {
			return err
		}
		hold, err := s.tasks.PrepareHold(ctx, tx, locked, o.terms.Deposit)
		if err != nil {
			return err
		}
		prepared = &hold
		return nil
	})
	return prepared, err
}

// ApplyStart records a START result and the associated changes in one transaction.
func (s *Service) ApplyStart(ctx context.Context, claimed task.Claim, result task.Result) (*task.Event, error) {
	var event *task.Event
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		o, locked, now, owned, err := s.lockOwnedStart(ctx, tx, claimed)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !owned {
			metrics.StaleClaims.WithLabelValues(string(claimed.Kind())).Inc()
			return nil
		}

		decision := o.decideStart(result.Outcome)
		switch {
		case decision.activate:
			if err := (repository{db: tx}).markOrderActive(ctx, o.id, now); err != nil {
				return err
			}
			event, err = s.tasks.CompleteStart(ctx, tx, locked, now, "", "")
			if err == nil {
				metrics.OrderActivation.Observe(now.Sub(o.createdAt).Seconds())
			}
		case decision.retry:
			return s.tasks.Retry(ctx, tx, locked, now, result.Detail)
		default:
			detail := ""
			if decision.reason == task.ContractViolation {
				detail = result.Detail
			}
			event, err = s.failStart(ctx, tx, o, locked, now, decision.reason, detail)
		}
		return err
	})
	return event, err
}

func (s *Service) lockOwnedStart(ctx context.Context, tx postgres.Tx, claimed task.Claim) (trip, task.Locked, time.Time, bool, error) {
	o, err := (repository{db: tx}).lockOrder(ctx, claimed.OrderID())
	if err != nil {
		return trip{}, task.Locked{}, time.Time{}, false, err
	}
	locked, err := s.tasks.LockAttempt(ctx, tx, claimed)
	if err != nil {
		return trip{}, task.Locked{}, time.Time{}, false, err
	}
	now, err := postgres.Now(ctx, tx)
	if err != nil {
		return trip{}, task.Locked{}, time.Time{}, false, err
	}
	return o, locked, now, s.tasks.Owns(locked, claimed, now) && o.acceptsStartResult(), nil
}

// FinalizeStart closes expired or exhausted work. A sweep supplies a task ID and skips locks.
func (s *Service) FinalizeStart(ctx context.Context, orderID uuid.UUID, sweepTaskID *uuid.UUID) (*task.Event, error) {
	var event *task.Event
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		q := repository{db: tx}
		lockOrder := q.lockOrder
		if sweepTaskID != nil {
			lockOrder = q.tryLockOrder
		}
		o, err := lockOrder(ctx, orderID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		locked, err := s.tasks.LockStart(ctx, tx, orderID, sweepTaskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		now, err := postgres.Now(ctx, tx)
		if err != nil {
			return err
		}

		reason := o.startFailureReason(s.tasks.StartClosure(locked, now))
		if reason == "" {
			return nil
		}
		if !o.acceptsStartFinalization() {
			s.log.Error("pending START for an order that is not starting", "order_id", orderID, "status", o.status)
			return nil
		}
		event, err = s.failStart(ctx, tx, o, locked, now, reason, "")
		return err
	})
	return event, err
}

func (s *Service) failStart(ctx context.Context, tx postgres.Tx, o trip, locked task.Locked, now time.Time, reason task.Reason, detail string) (*task.Event, error) {
	if o.acceptsStartResult() {
		closed := o.closeStart(now, s.configs.Current().Retention)
		if err := (repository{db: tx}).markOrderStartFailed(ctx, closed.id, *closed.closedAt, *closed.deleteAfter); err != nil {
			return nil, err
		}
	}
	if o.needsVoid(reason) {
		if err := s.tasks.EnqueueVoid(ctx, tx, o.id, now); err != nil {
			return nil, err
		}
	}
	return s.tasks.CompleteStart(ctx, tx, locked, now, reason, detail)
}
