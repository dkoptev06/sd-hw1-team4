// Package task owns durable work and the state used to apply worker results.
package task

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Kind string

const (
	Start  Kind = "START"
	Settle Kind = "SETTLE"
	Void   Kind = "VOID"
)

type Status string

const (
	Pending    Status = "PENDING"
	Completed  Status = "COMPLETED"
	DeadLetter Status = "DEAD_LETTER"
)

const startTTL = 2 * time.Minute
const holdGracePeriod = time.Hour

type StartAttempt struct {
	status       Status
	attemptCount int
	claimID      *uuid.UUID
	leaseUntil   *time.Time
	deadline     time.Time
}

func newStartAttempt(status Status, attemptCount int, claimID *uuid.UUID, leaseUntil *time.Time, deadline time.Time) (StartAttempt, error) {
	switch status {
	case Pending, Completed, DeadLetter:
	default:
		return StartAttempt{}, errors.New("invalid START task status")
	}
	if attemptCount < 0 || deadline.IsZero() || (claimID == nil) != (leaseUntil == nil) ||
		(claimID != nil && (*claimID == uuid.Nil || leaseUntil.IsZero())) {
		return StartAttempt{}, errors.New("invalid START task attempt count, deadline or lease")
	}
	return StartAttempt{status: status, attemptCount: attemptCount, claimID: claimID, leaseUntil: leaseUntil, deadline: deadline}, nil
}

// CanExpire checks whether the worker can finalize a pending START by deadline.
func (a StartAttempt) CanExpire(now time.Time) bool {
	return a.status == Pending && !now.Before(a.deadline)
}

// CanApply checks whether an ordinary worker result still owns the attempt.
func (a StartAttempt) CanApply(claimID uuid.UUID, now time.Time) bool {
	return a.status == Pending && a.claimID != nil && *a.claimID == claimID &&
		a.leaseUntil != nil && now.Before(*a.leaseUntil) && now.Before(a.deadline)
}

// CanExhaust reports whether the last allowed attempt has ended without a final result.
func (a StartAttempt) CanExhaust(now time.Time, maxAttempts int) bool {
	return a.status == Pending && a.attemptCount >= maxAttempts &&
		(a.leaseUntil == nil || !now.Before(*a.leaseUntil))
}
