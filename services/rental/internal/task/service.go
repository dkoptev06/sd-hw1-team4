package task

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

type Options struct {
	MaxAttempts MaxAttempts
	Lease       time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	Jitter      func() float64
}

type Service struct {
	repo repository
	opts Options
}

func NewService(db postgres.DBTX, opts Options) *Service {
	if opts.Jitter == nil {
		opts.Jitter = rand.Float64
	}
	return &Service{repo: repository{db: db}, opts: opts}
}

func (s *Service) EnqueueStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, userID string, deposit int64, now time.Time, ttl, grace time.Duration) error {
	deadline := now.Add(ttl)
	params := holdRecord{OperationID: orderID.String(), UserID: userID, Amount: deposit, HoldExpiresAt: deadline.Add(grace)}
	return s.enqueue(ctx, tx, orderID, Start, params, now, &deadline)
}

func (s *Service) EnqueueSettle(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, userID string, cost *big.Int, now time.Time) error {
	return s.enqueue(ctx, tx, orderID, Settle, settleRecord{OperationID: orderID.String(), UserID: userID, TotalAmount: cost}, now, nil)
}

func (s *Service) EnqueueVoid(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, now time.Time) error {
	return s.enqueue(ctx, tx, orderID, Void, voidRecord{OperationID: orderID.String()}, now, nil)
}

func (s *Service) enqueue(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, kind Kind, params any, now time.Time, deadline *time.Time) error {
	item, err := newEntry(orderID, kind, params, now, deadline)
	if err != nil {
		return err
	}
	return (repository{db: tx}).insert(ctx, item)
}

// Claim is a snapshot of a leased attempt. The service revalidates it against the locked task.
type Claim struct {
	value entry
	Lag   time.Duration
}

func (c Claim) ID() uuid.UUID      { return c.value.id }
func (c Claim) OrderID() uuid.UUID { return c.value.orderID }
func (c Claim) Kind() Kind         { return c.value.kind }
func (c Claim) AttemptCount() int  { return c.value.attempts }

func (s *Service) Claim(ctx context.Context, limit int) ([]Claim, error) {
	return s.repo.claim(ctx, limit, s.opts.Lease, s.opts.MaxAttempts)
}

// ReadyStats describes tasks that can currently be claimed.
type ReadyStats struct {
	Count             int64
	OldestWaitSeconds float64
}

// ReadyStats returns the count and oldest wait for each kind with claimable tasks.
func (s *Service) ReadyStats(ctx context.Context) (map[Kind]ReadyStats, error) {
	return s.repo.readyStats(ctx, s.opts.MaxAttempts)
}

// Sweep visits bounded pages of due finalizations, including work following locked rows.
func (s *Service) Sweep(ctx context.Context, batchSize int, process func(SweepTask) error) error {
	cutoff, err := postgres.Now(ctx, s.repo.db)
	if err != nil {
		return err
	}
	var firstErr error
	visit := func(tasks []SweepTask) {
		for _, candidate := range tasks {
			if err := process(candidate); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("finalize %s task %s: %w", candidate.Kind, candidate.ID, err)
			}
		}
	}
	var expiredAfter, exhaustedAfter SweepTask
	for ctx.Err() == nil {
		expired, err := s.repo.findExpiredStarts(ctx, batchSize, cutoff, expiredAfter)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		visit(expired)
		exhausted, err := s.repo.findExhausted(ctx, batchSize, s.opts.MaxAttempts, cutoff, exhaustedAfter)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		visit(exhausted)
		if len(expired) < batchSize && len(exhausted) < batchSize {
			return firstErr
		}
		if len(expired) > 0 {
			expiredAfter = expired[len(expired)-1]
		}
		if len(exhausted) > 0 {
			exhaustedAfter = exhausted[len(exhausted)-1]
		}
	}
	return errors.Join(firstErr, ctx.Err())
}

func (s *Service) ListDeadLetters(ctx context.Context) ([]DeadLetterEntry, error) {
	return s.repo.listDeadLetters(ctx)
}

func (s *Service) RequeueDeadLetters(ctx context.Context, id *uuid.UUID) (int64, error) {
	return s.repo.requeueDeadLetters(ctx, id)
}
