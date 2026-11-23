package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/config"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func (s *Service) lockStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (startingOrder, task.StartAttempt, error) {
	order, err := lockOrderForStartResult(ctx, tx, orderID)
	if err != nil {
		return startingOrder{}, task.StartAttempt{}, err
	}
	attempt, err := s.tasks.LockStart(ctx, tx, orderID)
	return order, attempt, err
}

// ApplyStart records the result of an ordinary worker attempt when the claim is still valid.
func (s *Service) ApplyStart(ctx context.Context, orderID, claimID uuid.UUID, result StartResult, retention config.Retention) (bool, error) {
	var applied bool
	err := s.tx.WithTx(ctx, func(tx postgres.Tx) error {
		order, attempt, err := s.lockStart(ctx, tx, orderID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		now, err := databaseTime(ctx, tx)
		if err != nil {
			return err
		}
		if !order.status.acceptsStartResult() || !attempt.CanApply(claimID, now) {
			return nil
		}
		switch result {
		case StartSucceeded:
			if err := markActive(ctx, tx, orderID, now); err != nil {
				return err
			}
			err = s.tasks.CompleteStart(ctx, tx, orderID, now, "success", nil)
		case PaymentRejected, ContractError:
			err = s.finishFailedStart(ctx, tx, orderID, order, now, string(result), result == ContractError, retention)
		default:
			return fmt.Errorf("unknown START result %q", result)
		}
		applied = err == nil
		return err
	})
	return applied && err == nil, err
}

// ExpireStart closes a pending START independently of its claim and retry schedule.
func (s *Service) ExpireStart(ctx context.Context, orderID uuid.UUID, retention config.Retention) (bool, error) {
	return s.finalizePendingStart(ctx, orderID, retention, func(_ Status, attempt task.StartAttempt, now time.Time) string {
		if attempt.CanExpire(now) {
			return deadlineExceeded
		}
		return ""
	})
}

// ExhaustStart finalizes a START with no attempts left when its last lease has ended.
// The deadline takes priority even when a lease is still present.
func (s *Service) ExhaustStart(ctx context.Context, orderID uuid.UUID, maxAttempts int, retention config.Retention) (bool, error) {
	if maxAttempts <= 0 {
		return false, errors.New("START attempt limit must be positive")
	}
	return s.finalizePendingStart(ctx, orderID, retention, func(status Status, attempt task.StartAttempt, now time.Time) string {
		return status.startFailureReason(attempt, now, maxAttempts)
	})
}

func (s *Service) finalizePendingStart(ctx context.Context, orderID uuid.UUID, retention config.Retention, reason func(Status, task.StartAttempt, time.Time) string) (bool, error) {
	var finalized bool
	err := s.tx.WithTx(ctx, func(tx postgres.Tx) error {
		order, attempt, err := s.lockStart(ctx, tx, orderID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !order.status.acceptsStartFailure() {
			return nil
		}
		now, err := databaseTime(ctx, tx)
		if err != nil {
			return err
		}
		failureReason := reason(order.status, attempt, now)
		if failureReason == "" {
			return nil
		}
		err = s.finishFailedStart(ctx, tx, orderID, order, now, failureReason, true, retention)
		finalized = err == nil
		return err
	})
	return finalized && err == nil, err
}

func (s *Service) finishFailedStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, order startingOrder, now time.Time, reason string, compensate bool, retention config.Retention) error {
	if order.status.acceptsStartResult() {
		if err := markStartFailed(ctx, tx, orderID, now, retention.DeleteAfter(now, order.zoneID)); err != nil {
			return err
		}
	}
	if compensate && order.deposit > 0 {
		if err := s.tasks.EnqueueVoid(ctx, tx, orderID, now); err != nil {
			return err
		}
	}
	return s.tasks.CompleteStart(ctx, tx, orderID, now, "failed", &reason)
}
