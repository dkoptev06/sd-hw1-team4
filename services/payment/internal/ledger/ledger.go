// Package ledger is the in-memory state machine of the Payment stub. Each
// operation_id is one payment cycle: HOLD → SETTLE, or VOID. Every command runs at
// most once per operation; a repeat with the same parameters returns the stored
// result, a repeat with other parameters is a conflict.
package ledger

import (
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalid           = errors.New("INVALID_REQUEST")
	ErrUserMismatch      = errors.New("USER_MISMATCH")
	ErrParameterConflict = errors.New("PARAMETER_CONFLICT")
	ErrInvalidOrder      = errors.New("INVALID_ORDER")
	ErrNotFound          = errors.New("NOT_FOUND")
)

// Results of commands.
const (
	Success = "success"
	Failed  = "failed"
)

// Statuses of a payment cycle.
const (
	StatusHeld        = "HELD"
	StatusHoldFailed  = "HOLD_FAILED"
	StatusHoldExpired = "HOLD_EXPIRED"
	StatusPaid        = "PAID"
	StatusDebt        = "DEBT"
	StatusVoided      = "VOIDED"
)

type hold struct {
	amount    int64
	expiresAt time.Time
	result    string
}

type settle struct {
	total  big.Int
	result string
}

type operation struct {
	userID string
	hold   *hold
	settle *settle
	voided bool
}

type Ledger struct {
	mu         sync.Mutex
	ops        map[string]*operation
	now        func() time.Time
	failSettle bool
}

func New(now func() time.Time, failSettle bool) *Ledger {
	return &Ledger{ops: map[string]*operation{}, now: now, failSettle: failSettle}
}

func (l *Ledger) op(id string) *operation {
	op, ok := l.ops[id]
	if !ok {
		op = &operation{}
		l.ops[id] = op
	}
	return op
}

func (l *Ledger) checkUser(id, userID string) error {
	if op, ok := l.ops[id]; ok && op.userID != "" && op.userID != userID {
		return ErrUserMismatch
	}
	return nil
}

// Hold reserves amount until expiresAt. The first HOLD fails after a VOID or when expiresAt already passed.
func (l *Ledger) Hold(operationID, userID string, amount int64, expiresAt time.Time) (string, error) {
	if blank(operationID) || blank(userID) || amount <= 0 || expiresAt.IsZero() {
		return "", ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkUser(operationID, userID); err != nil {
		return "", err
	}
	op := l.op(operationID)
	if op.hold != nil {
		if op.hold.amount != amount || !op.hold.expiresAt.Equal(expiresAt) {
			return "", ErrParameterConflict
		}
		return op.hold.result, nil
	}
	if op.settle != nil {
		return "", ErrInvalidOrder
	}
	result := Success
	if op.voided || !l.now().Before(expiresAt) {
		result = Failed
	}
	op.userID = userID
	op.hold = &hold{amount: amount, expiresAt: expiresAt, result: result}
	return result, nil
}

// Settle charges the final amount, with or without a hold, and releases the rest of a live hold.
func (l *Ledger) Settle(operationID, userID string, total *big.Int) (string, error) {
	if blank(operationID) || blank(userID) || total == nil || total.Sign() < 0 {
		return "", ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkUser(operationID, userID); err != nil {
		return "", err
	}
	op := l.op(operationID)
	if op.settle != nil {
		if op.settle.total.Cmp(total) != 0 {
			return "", ErrParameterConflict
		}
		return op.settle.result, nil
	}
	if op.voided {
		return "", ErrInvalidOrder
	}
	op.userID = userID
	result := Success
	if l.failSettle {
		result = Failed
	}
	op.settle = &settle{result: result}
	op.settle.total.Set(total)
	return result, nil
}

// Void cancels a hold and forbids a late first HOLD. It succeeds without a hold too.
func (l *Ledger) Void(operationID string) (string, error) {
	if blank(operationID) {
		return "", ErrInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	op := l.op(operationID)
	if op.voided {
		return Success, nil
	}
	if op.settle != nil {
		return "", ErrInvalidOrder
	}
	op.voided = true
	return Success, nil
}

// Status reads the state of a payment cycle.
func (l *Ledger) Status(operationID string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	op, ok := l.ops[operationID]
	switch {
	case !ok:
		return "", ErrNotFound
	case op.settle != nil && op.settle.result == Success:
		return StatusPaid, nil
	case op.settle != nil:
		return StatusDebt, nil
	case op.voided:
		return StatusVoided, nil
	case op.hold != nil && op.hold.result == Failed:
		return StatusHoldFailed, nil
	case op.hold != nil && !l.now().Before(op.hold.expiresAt):
		return StatusHoldExpired, nil
	case op.hold != nil:
		return StatusHeld, nil
	default:
		return "", ErrNotFound
	}
}

func blank(s string) bool { return strings.TrimSpace(s) == "" }
