// Package order owns the lifecycle and price of a rental trip.
package order

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

type Status string

const (
	Starting    Status = "STARTING"
	Active      Status = "ACTIVE"
	StartFailed Status = "START_FAILED"
	Completed   Status = "COMPLETED"
	Cancelled   Status = "CANCELLED"
)

var (
	ErrNotFound          = errors.New("NOT_FOUND")
	ErrScooterBusy       = errors.New("SCOOTER_BUSY")
	ErrInactive          = errors.New("ORDER_NOT_ACTIVE")
	ErrSourceUnavailable = errors.New("SOURCE_UNAVAILABLE")
)

const (
	minimumBillableDuration = 5 * time.Second
	deadlineExceeded        = "DEADLINE_EXCEEDED"
	maxAttemptsExceeded     = "MAX_ATTEMPTS_EXCEEDED"
)

type StartReceipt struct {
	OrderID  uuid.UUID
	Status   Status
	Replayed bool
}

type FinishReceipt struct {
	OrderID    uuid.UUID
	Status     Status
	FinishTime *time.Time
	Cost       *int64
}

type View struct {
	FinishReceipt
	StartedAt *time.Time
}

type StartResult string

const (
	StartSucceeded  StartResult = "success"
	PaymentRejected StartResult = "PAYMENT_REJECTED"
	ContractError   StartResult = "CONTRACT_ERROR"
)

func finishStatus(startedAt, finishedAt time.Time) Status {
	if finishedAt.Sub(startedAt) < minimumBillableDuration {
		return Cancelled
	}
	return Completed
}

func (s Status) canReplaceAt(now, deadline time.Time) bool {
	return s == Starting && !now.Before(deadline)
}

func (s Status) finishRequired() (bool, error) {
	switch s {
	case Active:
		return true, nil
	case Completed, Cancelled:
		return false, nil
	default:
		return false, ErrInactive
	}
}

func (s Status) acceptsStartResult() bool {
	return s == Starting
}

func (s Status) acceptsStartFailure() bool {
	return s == Starting || s == StartFailed
}

func (s Status) startFailureReason(attempt task.StartAttempt, now time.Time, maxAttempts int) string {
	if attempt.CanExpire(now) {
		return deadlineExceeded
	}
	if !attempt.CanExhaust(now, maxAttempts) {
		return ""
	}
	if s == StartFailed {
		return deadlineExceeded
	}
	return maxAttemptsExceeded
}
