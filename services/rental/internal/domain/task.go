package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskKind string

const (
	TaskStart  TaskKind = "START"
	TaskSettle TaskKind = "SETTLE"
	TaskVoid   TaskKind = "VOID"
)

type TaskStatus string

const (
	TaskPending    TaskStatus = "PENDING"
	TaskCompleted  TaskStatus = "COMPLETED"
	TaskDeadLetter TaskStatus = "DEAD_LETTER"
)

// Reason is the completion reason of an unsuccessful START.
type Reason string

const (
	ReasonPaymentRejected     Reason = "PAYMENT_REJECTED"
	ReasonDeadlineExceeded    Reason = "DEADLINE_EXCEEDED"
	ReasonMaxAttemptsExceeded Reason = "MAX_ATTEMPTS_EXCEEDED"
	ReasonContractError       Reason = "CONTRACT_ERROR"
)

// Task results stored in tasks.result.
const (
	ResultSuccess = "success"
	ResultFailed  = "failed"
)

// HoldParams are the immutable parameters of a START task.
type HoldParams struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

// SettleParams are the immutable parameters of a SETTLE task.
type SettleParams struct {
	OperationID string `json:"operation_id"`
	UserID      string `json:"user_id"`
	TotalAmount int64  `json:"total_amount"`
}

// VoidParams are the immutable parameters of a VOID task.
type VoidParams struct {
	OperationID string `json:"operation_id"`
}

// Task is a durable unit of background work bound to an order.
type Task struct {
	ID           uuid.UUID
	OrderID      uuid.UUID
	Kind         TaskKind
	Status       TaskStatus
	Params       json.RawMessage
	AttemptCount int
	ClaimID      *uuid.UUID
	LeaseUntil   *time.Time
	NextRunAt    time.Time
	Deadline     *time.Time
}

func (t Task) leaseLive(now time.Time) bool {
	return t.LeaseUntil != nil && now.Before(*t.LeaseUntil)
}

// Owns reports whether a worker holding claimID may still record the result of its attempt.
func (t Task) Owns(claimID uuid.UUID, now time.Time) bool {
	if t.Status != TaskPending || t.ClaimID == nil || *t.ClaimID != claimID || !t.leaseLive(now) {
		return false
	}
	return t.Deadline == nil || now.Before(*t.Deadline)
}

// DeadlinePassed reports whether a pending START must be closed regardless of its lease and schedule.
func (t Task) DeadlinePassed(now time.Time) bool {
	return t.Status == TaskPending && t.Deadline != nil && !now.Before(*t.Deadline)
}

// Exhausted reports whether the last allowed attempt ended without a result.
// A live lease means the last attempt may still succeed.
func (t Task) Exhausted(now time.Time, maxAttempts int) bool {
	return t.Status == TaskPending && t.AttemptCount >= maxAttempts && !t.leaseLive(now)
}

// StartFailureReason decides whether a pending START is finalized without another
// attempt, and with which reason. The deadline has priority over the attempt limit.
// An empty reason means the task must be left alone.
func StartFailureReason(task Task, order OrderStatus, now time.Time, maxAttempts int) Reason {
	switch {
	case task.DeadlinePassed(now):
		return ReasonDeadlineExceeded
	case !task.Exhausted(now, maxAttempts):
		return ""
	case order == OrderStartFailed:
		// The API already closed the order after its deadline.
		return ReasonDeadlineExceeded
	default:
		return ReasonMaxAttemptsExceeded
	}
}
