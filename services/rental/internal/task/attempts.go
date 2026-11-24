package task

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

// Locked is a task read while holding its row lock in the caller's transaction.
type Locked struct{ value entry }

func (s *Service) LockAttempt(ctx context.Context, tx postgres.Tx, claimed Claim) (Locked, error) {
	t, err := (repository{db: tx}).lock(ctx, claimed.ID())
	return Locked{value: t}, err
}

// LockStart locks the current START after the order. Sweeps pass a selected ID and skip locks.
func (s *Service) LockStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, sweepTaskID *uuid.UUID) (Locked, error) {
	repo := repository{db: tx}
	if sweepTaskID != nil {
		t, err := repo.tryLock(ctx, *sweepTaskID)
		return Locked{value: t}, err
	}
	t, err := repo.lockStart(ctx, orderID)
	return Locked{value: t}, err
}

func (*Service) Owns(locked Locked, claimed Claim, now time.Time) bool {
	return claimed.value.claimID != nil && locked.value.owns(*claimed.value.claimID, now)
}

func (s *Service) StartClosure(locked Locked, now time.Time) Reason {
	return locked.value.startClosure(now, s.opts.MaxAttempts[Start])
}

func (s *Service) PrepareHold(ctx context.Context, tx postgres.Tx, locked Locked, deposit int64) (PreparedHold, error) {
	hold, changed := locked.value.prepareHold(deposit)
	if changed {
		return hold, (repository{db: tx}).updateHold(ctx, locked.value.id, holdRecord(hold.Params))
	}
	return hold, nil
}

func (*Service) SettleParams(claimed Claim) (SettleParams, error) {
	var record settleRecord
	err := decodeParameters(claimed.value.params, &record)
	return SettleParams(record), err
}

func (*Service) VoidParams(claimed Claim) (VoidParams, error) {
	var record voidRecord
	err := decodeParameters(claimed.value.params, &record)
	return VoidParams(record), err
}

func (s *Service) CompleteStart(ctx context.Context, tx postgres.Tx, locked Locked, now time.Time, reason Reason, detail string) (*Event, error) {
	if reason == "" {
		return s.complete(ctx, tx, locked.value, now, ResultSuccess, nil, detail)
	}
	return s.complete(ctx, tx, locked.value, now, ResultFailed, &reason, detail)
}

func (s *Service) Retry(ctx context.Context, tx postgres.Tx, locked Locked, now time.Time, detail string) error {
	t := locked.value
	next := t.retryAt(now, s.opts.BackoffBase, s.opts.BackoffMax, s.opts.Jitter)
	return (repository{db: tx}).reschedule(ctx, t.id, next, detail)
}

// Event describes a completed transaction for worker metrics and logs.
type Event struct {
	ID, OrderID uuid.UUID
	Kind        Kind
	Status      Status
	Attempts    int
	Result      string
	Reason      Reason
	LastError   string
	Cause       string
}

func (t entry) event(status Status, lastError string) *Event {
	return &Event{ID: t.id, OrderID: t.orderID, Kind: t.kind, Status: status, Attempts: t.attempts, LastError: lastError}
}

func (*Service) complete(ctx context.Context, tx postgres.Tx, t entry, now time.Time, result string, reason *Reason, detail string) (*Event, error) {
	lastError, err := (repository{db: tx}).complete(ctx, t.id, now, result, reason, detail)
	if err != nil {
		return nil, err
	}
	event := t.event(Completed, lastError)
	event.Result = result
	if reason != nil {
		event.Reason = *reason
	}
	return event, nil
}

// ApplyPayment persists the result of a SETTLE or VOID attempt if its claim is still live.
func (s *Service) ApplyPayment(ctx context.Context, claimed Claim, result Result) (*Event, error) {
	var event *Event
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		locked, err := s.LockAttempt(ctx, tx, claimed)
		if err != nil {
			return err
		}
		now, err := postgres.Now(ctx, tx)
		if err != nil {
			return err
		}
		if !s.Owns(locked, claimed, now) {
			metrics.StaleClaims.WithLabelValues(string(claimed.Kind())).Inc()
			return nil
		}
		t := locked.value
		completion, retry, failure := t.paymentDisposition(result)
		switch {
		case completion != "":
			event, err = s.complete(ctx, tx, t, now, completion, nil, "")
		case retry:
			return s.Retry(ctx, tx, locked, now, result.Detail)
		default:
			event, err = s.deadLetter(ctx, tx, t, failure, "")
		}
		return err
	})
	return event, err
}

func (*Service) deadLetter(ctx context.Context, tx postgres.Tx, t entry, detail, cause string) (*Event, error) {
	lastError, err := (repository{db: tx}).deadLetter(ctx, t.id, detail)
	if err != nil {
		return nil, err
	}
	event := t.event(DeadLetter, lastError)
	event.Cause = cause
	return event, nil
}

func (s *Service) DeadLetterExhausted(ctx context.Context, id uuid.UUID) (*Event, error) {
	var event *Event
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		t, err := (repository{db: tx}).tryLock(ctx, id)
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
		if !t.exhausted(now, s.opts.MaxAttempts[t.kind]) {
			return nil
		}
		event, err = s.deadLetter(ctx, tx, t, "", "max attempts exceeded")
		return err
	})
	return event, err
}
