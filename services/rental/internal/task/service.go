package task

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

// Service manages durable tasks within a transaction owned by the caller.
type Service struct{}

// EnqueueStart records the immutable deadline and HOLD parameters during order creation.
func (Service) EnqueueStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, userID string, deposit int64, createdAt time.Time) error {
	deadline := createdAt.Add(startTTL)
	parameters := startParameters{
		OperationID:   orderID,
		UserID:        userID,
		Amount:        deposit,
		HoldExpiresAt: deadline.Add(holdGracePeriod),
	}
	return insertStart(ctx, tx, orderID, parameters, createdAt, deadline)
}

// EnqueueSettle participates in the transaction that closes the order.
func (Service) EnqueueSettle(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, userID string, cost int64, createdAt time.Time) error {
	return insert(ctx, tx, orderID, Settle, createdAt, settleParameters{OperationID: orderID, UserID: userID, TotalAmount: cost})
}

// EnqueueVoid participates in the transaction that records the failed start.
func (Service) EnqueueVoid(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, createdAt time.Time) error {
	return insert(ctx, tx, orderID, Void, createdAt, voidParameters{OperationID: orderID})
}

// StartDeadline reads the immutable deadline when the caller locks the order.
func (Service) StartDeadline(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (time.Time, error) {
	return startDeadline(ctx, tx, orderID)
}

// LockStart locks the START task after the caller has locked its order.
func (Service) LockStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (StartAttempt, error) {
	return lockStart(ctx, tx, orderID)
}

// CompleteStart finalizes the locked task and clears its lease.
func (Service) CompleteStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, now time.Time, result string, reason *string) error {
	return completeStart(ctx, tx, orderID, now, result, reason)
}
