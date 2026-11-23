package offer

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

type Service struct {
	db postgres.DBTX
	tx postgres.TxRunner
}

func NewService(db postgres.DBTX, tx postgres.TxRunner) *Service {
	return &Service{db: db, tx: tx}
}

// Lookup returns a quote, including expired offers until they are removed.
func (s *Service) Lookup(ctx context.Context, key, userID, scooterID string) (Receipt, error) {
	return lookup(ctx, s.db, key, userID, scooterID)
}

// Store persists a priced quote or returns the result of an earlier request.
func (s *Service) Store(ctx context.Context, quote Quote) (Receipt, error) {
	var receipt Receipt
	err := s.tx.WithTx(ctx, func(tx postgres.Tx) error {
		if err := lockIdempotencyKey(ctx, tx, quote.idempotencyID); err != nil {
			return err
		}
		existing, err := lookup(ctx, tx, quote.idempotencyID, quote.userID, quote.scooter.id)
		if !errors.Is(err, ErrNotFound) {
			receipt = existing
			return err
		}
		receipt, err = insert(ctx, tx, uuid.New(), quote)
		return err
	})
	if err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// FindScooterForStart reads the available offer without locking it across Source I/O.
func (s *Service) FindScooterForStart(ctx context.Context, offerID uuid.UUID) (string, error) {
	scooterID, status, expiresAt, now, err := findScooterForStart(ctx, s.db, offerID)
	if err != nil {
		return "", err
	}
	if err := checkAvailability(status, expiresAt, now); err != nil {
		return "", err
	}
	return scooterID, nil
}

// LockForStart reads and locks the quote in the caller's order-creation transaction.
func (s *Service) LockForStart(ctx context.Context, tx postgres.Tx, offerID uuid.UUID) (StartSnapshot, error) {
	return lockForStart(ctx, tx, offerID)
}

// Consume marks the offer used in the caller's order-creation transaction.
func (s *Service) Consume(ctx context.Context, tx postgres.Tx, offerID uuid.UUID) error {
	return consume(ctx, tx, offerID)
}
