// Package task owns Rental's durable START, SETTLE and VOID queue.
package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
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

type Reason string

const (
	PaymentRejected     Reason = "PAYMENT_REJECTED"
	DeadlineExceeded    Reason = "DEADLINE_EXCEEDED"
	MaxAttemptsExceeded Reason = "MAX_ATTEMPTS_EXCEEDED"
	ContractViolation   Reason = "CONTRACT_ERROR"
)

const (
	ResultSuccess = "success"
	ResultFailed  = "failed"
)

type Outcome int

const (
	Success Outcome = iota
	Rejected
	ContractError
	Transient
)

func (o Outcome) String() string {
	switch o {
	case Success:
		return "success"
	case Rejected:
		return "rejected"
	case ContractError:
		return "contract_error"
	default:
		return "transient"
	}
}

type Result struct {
	Outcome Outcome
	Detail  string
}

// Payment commands contain the values submitted to the Payment client.
type HoldParams struct {
	OperationID   string
	UserID        string
	Amount        int64
	HoldExpiresAt time.Time
}

type SettleParams struct {
	OperationID string
	UserID      string
	TotalAmount *big.Int
}

type VoidParams struct {
	OperationID string
}

type schedule struct {
	attempts   int
	claimID    *uuid.UUID
	leaseUntil *time.Time
	nextRunAt  time.Time
	deadline   *time.Time
}

type entry struct {
	id, orderID uuid.UUID
	kind        Kind
	status      Status
	params      json.RawMessage
	schedule
}

func restore(id, orderID uuid.UUID, kind Kind, status Status, params json.RawMessage, schedule schedule) (entry, error) {
	if id == uuid.Nil || orderID == uuid.Nil {
		return entry{}, errors.New("task and order IDs are required")
	}
	switch kind {
	case Start, Settle, Void:
	default:
		return entry{}, fmt.Errorf("unknown task kind %q", kind)
	}
	switch status {
	case Pending, Completed, DeadLetter:
	default:
		return entry{}, fmt.Errorf("unknown task status %q", status)
	}
	if (kind == Start) != (schedule.deadline != nil) || (schedule.claimID == nil) != (schedule.leaseUntil == nil) ||
		schedule.attempts < 0 || schedule.nextRunAt.IsZero() {
		return entry{}, errors.New("invalid task schedule")
	}
	return entry{id: id, orderID: orderID, kind: kind, status: status, params: params, schedule: schedule}, nil
}

func newEntry(orderID uuid.UUID, kind Kind, params any, now time.Time, deadline *time.Time) (entry, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return entry{}, err
	}
	return restore(uuid.New(), orderID, kind, Pending, raw, schedule{nextRunAt: now, deadline: deadline})
}

func (t entry) leaseLive(now time.Time) bool {
	return t.leaseUntil != nil && now.Before(*t.leaseUntil)
}

func (t entry) owns(claimID uuid.UUID, now time.Time) bool {
	return t.status == Pending && t.claimID != nil && *t.claimID == claimID && t.leaseLive(now) &&
		(t.deadline == nil || now.Before(*t.deadline))
}

func (t entry) deadlinePassed(now time.Time) bool {
	return t.status == Pending && t.deadline != nil && !now.Before(*t.deadline)
}

func (t entry) exhausted(now time.Time, maxAttempts int) bool {
	return t.status == Pending && t.attempts >= maxAttempts && !t.leaseLive(now)
}

func (t entry) startClosure(now time.Time, maxAttempts int) Reason {
	switch {
	case t.deadlinePassed(now):
		return DeadlineExceeded
	case t.exhausted(now, maxAttempts):
		return MaxAttemptsExceeded
	default:
		return ""
	}
}

func (t entry) retryAt(now time.Time, base, maxDelay time.Duration, jitter func() float64) time.Time {
	next := now.Add(backoff(t.attempts, base, maxDelay, jitter))
	if t.deadline != nil && next.After(*t.deadline) {
		return *t.deadline
	}
	return next
}

type PreparedHold struct {
	Params HoldParams
	Detail string
}

func (t entry) prepareHold(deposit int64) (PreparedHold, bool) {
	var stored holdRecord
	if err := decodeParameters(t.params, &stored); err != nil {
		return PreparedHold{Detail: err.Error()}, false
	}
	hold := PreparedHold{Params: HoldParams(stored)}
	if stored.Amount == deposit {
		return hold, false
	}
	hold.Params.Amount = deposit
	if t.attempts > 1 {
		hold.Detail = fmt.Sprintf("HOLD amount %d differs from order deposit %d after a prior attempt; reconcile in Payment before retrying",
			stored.Amount, deposit)
		return hold, false
	}
	return hold, true
}

func (t entry) paymentDisposition(result Result) (completion string, retry bool, failure string) {
	switch {
	case result.Outcome == Success:
		return ResultSuccess, false, ""
	case result.Outcome == Rejected && t.kind == Settle:
		return ResultFailed, false, ""
	case result.Outcome == Transient:
		return "", true, ""
	default:
		detail := result.Detail
		if detail == "" {
			detail = "unexpected result " + result.Outcome.String()
		}
		return "", false, detail
	}
}
