package order

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

type scooters interface {
	Scooter(context.Context, string) (source.Scooter, error)
}

type configs interface {
	Current() dynconfig.Snapshot
}

type Service struct {
	repo      repository
	cache     *archiveCache
	offers    *offer.Service
	tasks     *task.Service
	scooters  scooters
	configs   configs
	startTTL  time.Duration
	holdGrace time.Duration
	log       *slog.Logger
}

func NewService(db postgres.DBTX, offers *offer.Service, tasks *task.Service, scooters scooters, configs configs, startTTL, holdGrace time.Duration, log *slog.Logger) *Service {
	return &Service{repo: repository{db: db}, offers: offers, tasks: tasks, scooters: scooters,
		configs: configs, startTTL: startTTL, holdGrace: holdGrace, log: log}
}

// WithArchiveCache configures API reads before the service starts serving requests.
func (s *Service) WithArchiveCache(cfg CacheOptions, remote RemoteCache) (*Service, error) {
	cache, err := newArchiveCache(cfg, remote, s.repo.getOrder, time.Now)
	if err != nil {
		return nil, err
	}
	configured := *s
	configured.cache = cache
	return &configured, nil
}

func (s *Service) Close() error {
	if s.cache == nil {
		return nil
	}
	return s.cache.close()
}

// StartResult is the answer to POST /orders.
type StartResult struct {
	OrderID uuid.UUID
	Status  Status
	// Replayed is true when the order already existed for the offer.
	Replayed bool
}

// Start creates a STARTING order and its START task from an offer. A repeated
// request for the same offer returns the existing order and its current status.
func (s *Service) Start(ctx context.Context, offerID uuid.UUID) (StartResult, error) {
	if result, err := findStartReplay(ctx, s.repo, offerID); !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}

	scooterID, err := s.offers.FindScooterForStart(ctx, offerID)
	if errors.Is(err, offer.ErrNotFound) || errors.Is(err, offer.ErrOfferExpired) {
		return s.replayOr(ctx, offerID, err)
	}
	if err != nil {
		return StartResult{}, err
	}

	// Source is read before the transaction so no row locks are held while waiting.
	current, err := s.scooters.Scooter(ctx, scooterID)
	if errors.Is(err, source.ErrNotFound) {
		return s.replayOr(ctx, offerID, fmt.Errorf("%w: scooter %q no longer exists", offer.ErrOfferChanged, scooterID))
	}
	if err != nil {
		return s.replayOr(ctx, offerID, fmt.Errorf("%w: scooter %q: %v", ErrSourceUnavailable, scooterID, err))
	}

	var result StartResult
	err = postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		var txErr error
		result, txErr = s.startInTx(ctx, tx, offerID, current)
		return txErr
	})
	if err != nil {
		return StartResult{}, err
	}
	return result, nil
}

func (s *Service) startInTx(ctx context.Context, tx postgres.Tx, offerID uuid.UUID, current source.Scooter) (StartResult, error) {
	locked, lockErr := s.offers.LockForStart(ctx, tx, offerID)
	if lockErr != nil && !errors.Is(lockErr, offer.ErrNotFound) {
		return StartResult{}, lockErr
	}
	q := repository{db: tx}
	// The order could have been created or archived since the initial lookup.
	if result, err := findStartReplay(ctx, q, offerID); !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	if lockErr != nil {
		return StartResult{}, lockErr
	}

	scooterID := locked.ScooterID()
	if err := q.lockScooterForStart(ctx, scooterID); err != nil {
		return StartResult{}, err
	}
	occupying, previousDeadline, err := q.lockOccupyingOrder(ctx, scooterID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return StartResult{}, err
	}
	occupied := err == nil

	now, err := postgres.Now(ctx, tx)
	if err != nil {
		return StartResult{}, err
	}
	terms, err := s.offers.CheckStart(locked, now, current)
	if err != nil {
		return StartResult{}, err
	}
	if occupied {
		if !occupying.canBeReplacedAt(now, previousDeadline) {
			return StartResult{}, fmt.Errorf("%w: scooter %q has order %s", ErrScooterBusy, scooterID, occupying.id)
		}
		// The worker will close the previous START task and issue any VOID.
		closed := occupying.closeStart(now, s.configs.Current().Retention)
		if err := q.markOrderStartFailed(ctx, closed.id, *closed.closedAt, *closed.deleteAfter); err != nil {
			return StartResult{}, err
		}
	}

	if err := s.offers.Consume(ctx, tx, locked); err != nil {
		return StartResult{}, err
	}
	o, err := newTrip(offerID, terms.UserID, terms.Scooter, terms.Terms, now)
	if err != nil {
		return StartResult{}, err
	}
	if err := q.insertOrder(ctx, o); err != nil {
		return StartResult{}, err
	}
	if err := s.tasks.EnqueueStart(ctx, tx, o.id, o.userID, o.terms.Deposit, now, s.startTTL, s.holdGrace); err != nil {
		return StartResult{}, err
	}
	return StartResult{OrderID: o.id, Status: o.status}, nil
}

func findStartReplay(ctx context.Context, q repository, offerID uuid.UUID) (StartResult, error) {
	result, err := q.findOrderByOffer(ctx, offerID)
	result.Replayed = err == nil
	return result, err
}

// replayOr answers a retry that raced with a successful start; otherwise it returns fallback.
func (s *Service) replayOr(ctx context.Context, offerID uuid.UUID, fallback error) (StartResult, error) {
	result, err := findStartReplay(ctx, s.repo, offerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return StartResult{}, fallback
	}
	return result, err
}

// View is the read-only result of looking up an order.
type View struct {
	ID         uuid.UUID
	Status     Status
	Cost       *big.Int
	StartedAt  *time.Time
	FinishTime *time.Time
	Archived   bool
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (View, error) {
	read := s.repo.getOrder
	if s.cache != nil {
		read = s.cache.get
	}
	o, archived, now, err := read(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, fmt.Errorf("%w: order %s not found", ErrNotFound, id)
	}
	if err != nil {
		return View{}, err
	}
	cost, err := o.costAt(now)
	if err != nil {
		return View{}, fmt.Errorf("calculate cost for order %s: %w", id, err)
	}
	return View{ID: o.id, Status: o.status, Cost: cost, StartedAt: utc(o.startedAt), FinishTime: utc(o.finishTime), Archived: archived}, nil
}

type FinishReceipt struct {
	ID         uuid.UUID
	Status     Status
	FinishTime *time.Time
	Cost       *big.Int
}

func (s *Service) Finish(ctx context.Context, orderID uuid.UUID) (FinishReceipt, error) {
	var o trip
	err := postgres.WithTx(ctx, s.repo.db, func(tx postgres.Tx) error {
		var err error
		o, err = s.finishInTx(ctx, tx, orderID)
		return err
	})
	if err != nil {
		return FinishReceipt{}, err
	}
	return FinishReceipt{ID: o.id, Status: o.status, FinishTime: o.finishTime, Cost: copyCost(o.cost)}, nil
}

func (s *Service) finishInTx(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (trip, error) {
	q := repository{db: tx}
	o, err := q.lockOrder(ctx, orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		o, err = q.getArchivedOrder(ctx, orderID)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return trip{}, fmt.Errorf("%w: order %s not found", ErrNotFound, orderID)
	}
	if err != nil {
		return trip{}, err
	}

	required, err := o.finishRequired()
	if err != nil || !required {
		return o, err
	}
	now, err := postgres.Now(ctx, tx)
	if err != nil {
		return trip{}, err
	}
	o, err = o.finish(now, s.configs.Current().Retention)
	if err != nil {
		return trip{}, err
	}
	if err := q.markOrderFinished(ctx, o.id, o.status, *o.finishTime, *o.deleteAfter, o.cost); err != nil {
		return trip{}, err
	}
	if err := s.tasks.EnqueueSettle(ctx, tx, o.id, o.userID, o.cost, now); err != nil {
		return trip{}, err
	}
	return o, nil
}
