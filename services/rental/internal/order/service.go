package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/config"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

// ScooterSource provides fresh scooter data. An unknown scooter returns ErrNotFound.
type ScooterSource interface {
	GetScooter(ctx context.Context, scooterID string) (offer.ScooterSnapshot, error)
}

type Service struct {
	db     postgres.DBTX
	tx     postgres.TxRunner
	offers *offer.Service
	tasks  task.Service
}

func NewService(db postgres.DBTX, tx postgres.TxRunner, offers *offer.Service, tasks task.Service) *Service {
	return &Service{db: db, tx: tx, offers: offers, tasks: tasks}
}

// FindOrderForOffer is the retry path before an HTTP request to Source.
func (s *Service) FindOrderForOffer(ctx context.Context, offerID uuid.UUID) (StartReceipt, error) {
	return findOrderForOffer(ctx, s.db, offerID)
}

// StartOrder reads Source before committing the offer, new order and START task together.
func (s *Service) StartOrder(ctx context.Context, offerID uuid.UUID, scooters ScooterSource, retention config.Retention) (StartReceipt, error) {
	replay, err := s.FindOrderForOffer(ctx, offerID)
	if !errors.Is(err, ErrNotFound) {
		return replay, err
	}
	scooterID, err := s.offers.FindScooterForStart(ctx, offerID)
	if errors.Is(err, offer.ErrNotFound) {
		return s.replayAfterStartError(ctx, offerID, ErrNotFound)
	}
	if errors.Is(err, offer.ErrExpired) {
		return s.replayAfterStartError(ctx, offerID, err)
	}
	if err != nil {
		return StartReceipt{}, err
	}

	current, err := scooters.GetScooter(ctx, scooterID)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			err = fmt.Errorf("%w: %w", ErrSourceUnavailable, err)
		}
		return s.replayAfterStartError(ctx, offerID, err)
	}

	var receipt StartReceipt
	err = s.tx.WithTx(ctx, func(tx postgres.Tx) error {
		var err error
		receipt, err = s.startOrder(ctx, tx, offerID, current, retention)
		return err
	})
	if err != nil {
		return StartReceipt{}, err
	}
	return receipt, nil
}

// Another request may have committed the order while this request was reading the offer or Source.
func (s *Service) replayAfterStartError(ctx context.Context, offerID uuid.UUID, fallback error) (StartReceipt, error) {
	replay, err := s.FindOrderForOffer(ctx, offerID)
	if errors.Is(err, ErrNotFound) {
		return StartReceipt{}, fallback
	}
	return replay, err
}

func (s *Service) startOrder(ctx context.Context, tx postgres.Tx, offerID uuid.UUID, current offer.ScooterSnapshot, retention config.Retention) (StartReceipt, error) {
	snapshot, offerErr := s.offers.LockForStart(ctx, tx, offerID)
	if offerErr != nil && !errors.Is(offerErr, offer.ErrNotFound) {
		return StartReceipt{}, offerErr
	}
	replay, err := findOrderForOffer(ctx, tx, offerID)
	if !errors.Is(err, ErrNotFound) {
		return replay, err
	}
	if offerErr != nil {
		return StartReceipt{}, ErrNotFound
	}

	if err := lockScooter(ctx, tx, snapshot.ScooterID()); err != nil {
		return StartReceipt{}, err
	}
	previous, err := lockOccupiedOrder(ctx, tx, snapshot.ScooterID())
	if err != nil {
		return StartReceipt{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return StartReceipt{}, err
	}
	if err := snapshot.CanStart(now, current); err != nil {
		return StartReceipt{}, err
	}
	if previous.status != "" {
		var deadline time.Time
		if previous.status == Starting {
			deadline, err = s.tasks.StartDeadline(ctx, tx, previous.id)
			if err != nil {
				return StartReceipt{}, err
			}
		}
		if !previous.status.canReplaceAt(now, deadline) {
			return StartReceipt{}, ErrScooterBusy
		}
		if err := markStartFailed(ctx, tx, previous.id, now, retention.DeleteAfter(now, previous.zoneID)); err != nil {
			return StartReceipt{}, err
		}
	}

	orderID := uuid.New()
	if err := s.offers.Consume(ctx, tx, offerID); err != nil {
		return StartReceipt{}, err
	}
	if err := insertOrder(ctx, tx, orderID, offerID, snapshot, now); err != nil {
		return StartReceipt{}, err
	}
	if err := s.tasks.EnqueueStart(ctx, tx, orderID, snapshot.UserID(), snapshot.Deposit(), now); err != nil {
		return StartReceipt{}, err
	}
	return StartReceipt{OrderID: orderID, Status: Starting}, nil
}

// GetOrder reads current or archived state and calculates the price for active trips.
func (s *Service) GetOrder(ctx context.Context, orderID uuid.UUID) (View, error) {
	return getOrder(ctx, s.db, orderID)
}

// FinishOrder locks and closes the order with a single durable SETTLE task.
func (s *Service) FinishOrder(ctx context.Context, orderID uuid.UUID, retention config.Retention) (FinishReceipt, error) {
	var receipt FinishReceipt
	err := s.tx.WithTx(ctx, func(tx postgres.Tx) error {
		var err error
		receipt, err = s.finishOrder(ctx, tx, orderID, retention)
		return err
	})
	if err != nil {
		return FinishReceipt{}, err
	}
	return receipt, nil
}

func (s *Service) finishOrder(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, retention config.Retention) (FinishReceipt, error) {
	state, err := lockOrderForFinish(ctx, tx, orderID)
	if err != nil {
		return FinishReceipt{}, err
	}
	required, err := state.receipt.Status.finishRequired()
	if err != nil {
		return FinishReceipt{}, err
	}
	if !required {
		return state.receipt, nil
	}

	now, err := databaseTime(ctx, tx)
	if err != nil {
		return FinishReceipt{}, err
	}
	if state.startedAt == nil {
		return FinishReceipt{}, fmt.Errorf("active order %s is missing started_at", orderID)
	}
	cost, err := tripCost(ctx, tx, state.minuteRate, state.unlockFee, now, *state.startedAt)
	if err != nil {
		return FinishReceipt{}, err
	}
	status := finishStatus(*state.startedAt, now)
	if err := markFinished(ctx, tx, orderID, status, now, retention.DeleteAfter(now, state.zoneID), cost); err != nil {
		return FinishReceipt{}, err
	}
	if err := s.tasks.EnqueueSettle(ctx, tx, orderID, state.userID, cost, now); err != nil {
		return FinishReceipt{}, err
	}
	return FinishReceipt{OrderID: orderID, Status: status, FinishTime: &now, Cost: &cost}, nil
}
