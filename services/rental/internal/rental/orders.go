package rental

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

type OrderService struct {
	store     *storage.Store
	scooters  Scooters
	configs   Configs
	startTTL  time.Duration
	holdGrace time.Duration
}

func NewOrderService(store *storage.Store, scooters Scooters, configs Configs, startTTL, holdGrace time.Duration) *OrderService {
	return &OrderService{store: store, scooters: scooters, configs: configs, startTTL: startTTL, holdGrace: holdGrace}
}

// StartResult is the answer to POST /orders.
type StartResult struct {
	OrderID uuid.UUID
	Status  domain.OrderStatus
	// Replayed is true when the order already existed for the offer.
	Replayed bool
}

// Start creates a STARTING order and its START task from an offer. A repeated
// request for the same offer returns the existing order and its current status.
func (s *OrderService) Start(ctx context.Context, offerID uuid.UUID) (StartResult, error) {
	if result, err := findStartReplay(ctx, &s.store.Queries, offerID); !errors.Is(err, storage.ErrNotFound) {
		return result, err
	}

	offer, err := s.store.GetOffer(ctx, offerID)
	if errors.Is(err, storage.ErrNotFound) {
		return s.replayOr(ctx, offerID, domain.Wrap(domain.ErrNotFound, "offer %s not found", offerID))
	}
	if err != nil {
		return StartResult{}, err
	}
	if offer.Status != domain.OfferAvailable {
		// Consumed by a concurrent start that committed after the first lookup.
		return s.replayOr(ctx, offerID, domain.ErrOfferExpired)
	}

	// Source is read before the transaction so no row locks are held while waiting.
	current, err := s.scooters.Scooter(ctx, offer.Scooter.ID)
	if errors.Is(err, source.ErrNotFound) {
		return s.replayOr(ctx, offerID, domain.Wrap(domain.ErrOfferChanged, "scooter %q no longer exists", offer.Scooter.ID))
	}
	if err != nil {
		return s.replayOr(ctx, offerID, domain.Wrap(domain.ErrSourceUnavailable, "scooter %q: %v", offer.Scooter.ID, err))
	}

	var result StartResult
	err = s.store.InTx(ctx, func(q *storage.Queries) error {
		var txErr error
		result, txErr = s.startInTx(ctx, q, offerID, current)
		return txErr
	})
	if err != nil {
		return StartResult{}, err
	}
	return result, nil
}

func (s *OrderService) startInTx(ctx context.Context, q *storage.Queries, offerID uuid.UUID, current domain.Scooter) (StartResult, error) {
	offer, lockErr := q.LockOffer(ctx, offerID)
	if lockErr != nil && !errors.Is(lockErr, storage.ErrNotFound) {
		return StartResult{}, lockErr
	}
	// The order could have been created or archived since the initial lookup.
	if result, err := findStartReplay(ctx, q, offerID); !errors.Is(err, storage.ErrNotFound) {
		return result, err
	}
	if lockErr != nil {
		return StartResult{}, domain.Wrap(domain.ErrNotFound, "offer %s not found", offerID)
	}

	if err := q.LockScooterForStart(ctx, offer.Scooter.ID); err != nil {
		return StartResult{}, err
	}
	occupying, previousDeadline, err := q.LockOccupyingOrder(ctx, offer.Scooter.ID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return StartResult{}, err
	}
	occupied := err == nil

	now, err := q.Now(ctx)
	if err != nil {
		return StartResult{}, err
	}
	if err := offer.CheckStart(now, current); err != nil {
		return StartResult{}, err
	}
	if occupied {
		if previousDeadline == nil || !occupying.CanBeReplacedAt(now, *previousDeadline) {
			return StartResult{}, domain.Wrap(domain.ErrScooterBusy, "scooter %q has order %s", offer.Scooter.ID, occupying.ID)
		}
		// The worker will close the previous START task and issue any VOID.
		deleteAfter := s.configs.Current().Retention.DeleteAfter(now, occupying.ZoneID)
		if err := q.MarkOrderStartFailed(ctx, occupying.ID, now, deleteAfter); err != nil {
			return StartResult{}, err
		}
	}

	if err := q.ConsumeOffer(ctx, offerID); err != nil {
		return StartResult{}, err
	}
	order := domain.NewOrder(offer, now)
	if err := q.InsertOrder(ctx, order); err != nil {
		return StartResult{}, err
	}
	deadline := now.Add(s.startTTL)
	hold := domain.HoldParams{
		OperationID:   order.ID.String(),
		UserID:        order.UserID,
		Amount:        order.Terms.Deposit,
		HoldExpiresAt: deadline.Add(s.holdGrace),
	}
	if err := q.InsertTask(ctx, order.ID, domain.TaskStart, hold, now, &deadline); err != nil {
		return StartResult{}, err
	}
	return StartResult{OrderID: order.ID, Status: order.Status}, nil
}

func findStartReplay(ctx context.Context, q *storage.Queries, offerID uuid.UUID) (StartResult, error) {
	ref, err := q.FindOrderByOffer(ctx, offerID)
	if err != nil {
		return StartResult{}, err
	}
	return StartResult{OrderID: ref.ID, Status: ref.Status, Replayed: true}, nil
}

// replayOr answers a retry that raced with a successful start; otherwise it returns fallback.
func (s *OrderService) replayOr(ctx context.Context, offerID uuid.UUID, fallback error) (StartResult, error) {
	result, err := findStartReplay(ctx, &s.store.Queries, offerID)
	if errors.Is(err, storage.ErrNotFound) {
		return StartResult{}, fallback
	}
	return result, err
}

// OrderView is the answer to GET /orders/{id}.
type OrderView struct {
	Order    domain.Order
	Cost     *big.Int
	Archived bool
}

// Get reads a current or archived order and calculates the cost of an active trip.
func (s *OrderService) Get(ctx context.Context, orderID uuid.UUID) (OrderView, error) {
	order, archived, now, err := s.store.GetOrder(ctx, orderID)
	if errors.Is(err, storage.ErrNotFound) {
		return OrderView{}, domain.Wrap(domain.ErrNotFound, "order %s not found", orderID)
	}
	if err != nil {
		return OrderView{}, err
	}
	cost, err := order.CostAt(now)
	if err != nil {
		return OrderView{}, fmt.Errorf("calculate cost for order %s: %w", orderID, err)
	}
	return OrderView{Order: order, Cost: cost, Archived: archived}, nil
}

// Finish closes an ACTIVE trip, fixes its cost and enqueues SETTLE. A repeated
// request returns the stored result.
func (s *OrderService) Finish(ctx context.Context, orderID uuid.UUID) (domain.Order, error) {
	var order domain.Order
	err := s.store.InTx(ctx, func(q *storage.Queries) error {
		var err error
		order, err = s.finishInTx(ctx, q, orderID)
		return err
	})
	return order, err
}

func (s *OrderService) finishInTx(ctx context.Context, q *storage.Queries, orderID uuid.UUID) (domain.Order, error) {
	order, err := q.LockOrder(ctx, orderID)
	if errors.Is(err, storage.ErrNotFound) {
		order, err = q.GetArchivedOrder(ctx, orderID)
	}
	if errors.Is(err, storage.ErrNotFound) {
		return domain.Order{}, domain.Wrap(domain.ErrNotFound, "order %s not found", orderID)
	}
	if err != nil {
		return domain.Order{}, err
	}

	switch {
	case order.Status.IsFinished():
		return order, nil
	case order.Status != domain.OrderActive:
		return domain.Order{}, domain.Wrap(domain.ErrOrderNotActive, "order %s is %s", orderID, order.Status)
	case order.StartedAt == nil:
		return domain.Order{}, fmt.Errorf("active order %s has no started_at", orderID)
	}

	now, err := q.Now(ctx)
	if err != nil {
		return domain.Order{}, err
	}
	status, cost, err := domain.FinishOutcome(order.Terms, *order.StartedAt, now)
	if err != nil {
		return domain.Order{}, fmt.Errorf("finish order %s: %w", orderID, err)
	}
	deleteAfter := s.configs.Current().Retention.DeleteAfter(now, order.ZoneID)
	if err := q.MarkOrderFinished(ctx, orderID, status, now, deleteAfter, cost); err != nil {
		return domain.Order{}, err
	}
	settle := domain.SettleParams{OperationID: orderID.String(), UserID: order.UserID, TotalAmount: cost}
	if err := q.InsertTask(ctx, orderID, domain.TaskSettle, settle, now, nil); err != nil {
		return domain.Order{}, err
	}
	order.Status, order.Cost = status, cost
	order.FinishTime, order.ClosedAt, order.DeleteAfter = &now, &now, &deleteAfter
	return order, nil
}
