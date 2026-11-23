package task

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

type startParameters struct {
	OperationID   uuid.UUID `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

type settleParameters struct {
	OperationID uuid.UUID `json:"operation_id"`
	UserID      string    `json:"user_id"`
	TotalAmount int64     `json:"total_amount"`
}

type voidParameters struct {
	OperationID uuid.UUID `json:"operation_id"`
}

func insertStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, parameters startParameters, createdAt, deadline time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tasks (task_id, order_id, task_type, parameters, created_at, next_run_at, deadline_at, hold_expires_at)
		VALUES ($1, $2, 'START', $3, $4, $4, $5, $6)`, uuid.New(), orderID,
		parameters, createdAt, deadline, parameters.HoldExpiresAt)
	return err
}

func insert(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, kind Kind, createdAt time.Time, parameters any) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tasks (task_id, order_id, task_type, parameters, created_at, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $5)`, uuid.New(), orderID, kind, parameters, createdAt)
	return err
}

func startDeadline(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (time.Time, error) {
	var deadline time.Time
	err := tx.QueryRow(ctx, `SELECT deadline_at FROM tasks WHERE order_id = $1 AND task_type = 'START'`, orderID).
		Scan(&deadline)
	return deadline, err
}

func lockStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (StartAttempt, error) {
	var status Status
	var attemptCount int
	var claimID *uuid.UUID
	var leaseUntil *time.Time
	var deadline time.Time
	err := tx.QueryRow(ctx, `
		SELECT status, attempt_count, claim_id, lease_until, deadline_at FROM tasks WHERE order_id = $1 AND task_type = 'START' FOR UPDATE`, orderID).
		Scan(&status, &attemptCount, &claimID, &leaseUntil, &deadline)
	if err != nil {
		return StartAttempt{}, err
	}
	return newStartAttempt(status, attemptCount, claimID, leaseUntil, deadline)
}

func completeStart(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, now time.Time, result string, reason *string) error {
	_, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'COMPLETED', finished_at = $2, result = $3, completion_reason = $4,
			claim_id = NULL, lease_until = NULL
		WHERE order_id = $1 AND task_type = 'START'`, orderID, now, result, reason)
	return err
}
