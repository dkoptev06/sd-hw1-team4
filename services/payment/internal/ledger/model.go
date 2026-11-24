package ledger

import (
	"errors"
	"math/big"
	"strings"
	"time"
)

var (
	errInvalid           = errors.New("INVALID_REQUEST")
	errUserMismatch      = errors.New("USER_MISMATCH")
	errParameterConflict = errors.New("PARAMETER_CONFLICT")
	errInvalidOrder      = errors.New("INVALID_ORDER")
	errNotFound          = errors.New("NOT_FOUND")
)

const (
	Success = "success"
	Failed  = "failed"
)

const (
	statusHeld        = "HELD"
	statusHoldFailed  = "HOLD_FAILED"
	statusHoldExpired = "HOLD_EXPIRED"
	statusPaid        = "PAID"
	statusDebt        = "DEBT"
	statusVoided      = "VOIDED"
)

type operationID string

func newOperationID(value string) (operationID, error) {
	if strings.TrimSpace(value) == "" {
		return "", errInvalid
	}
	return operationID(value), nil
}

type holdCommand struct {
	id        operationID
	userID    string
	amount    int64
	expiresAt time.Time
}

func newHoldCommand(id, userID string, amount int64, expiresAt time.Time) (holdCommand, error) {
	key, err := newOperationID(id)
	if err != nil || strings.TrimSpace(userID) == "" || amount <= 0 || expiresAt.IsZero() {
		return holdCommand{}, errInvalid
	}
	return holdCommand{id: key, userID: userID, amount: amount, expiresAt: expiresAt}, nil
}

type settleCommand struct {
	id     operationID
	userID string
	total  *big.Int
}

func newSettleCommand(id, userID string, total *big.Int) (settleCommand, error) {
	key, err := newOperationID(id)
	if err != nil || strings.TrimSpace(userID) == "" || total == nil || total.Sign() < 0 {
		return settleCommand{}, errInvalid
	}
	return settleCommand{id: key, userID: userID, total: total}, nil
}

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

func (o *operation) checkUser(userID string) error {
	if o.userID != "" && o.userID != userID {
		return errUserMismatch
	}
	return nil
}

// applyHold replays an earlier HOLD even after SETTLE. A first HOLD can fail after VOID or expiry.
func (o *operation) applyHold(cmd holdCommand, now func() time.Time) (string, error) {
	if err := o.checkUser(cmd.userID); err != nil {
		return "", err
	}
	if o.hold != nil {
		if o.hold.amount != cmd.amount || !o.hold.expiresAt.Equal(cmd.expiresAt) {
			return "", errParameterConflict
		}
		return o.hold.result, nil
	}
	if o.settle != nil {
		return "", errInvalidOrder
	}
	result := Success
	if o.voided || !now().Before(cmd.expiresAt) {
		result = Failed
	}
	o.userID = cmd.userID
	o.hold = &hold{amount: cmd.amount, expiresAt: cmd.expiresAt, result: result}
	return result, nil
}

func (o *operation) applySettle(cmd settleCommand, result string) (string, error) {
	if err := o.checkUser(cmd.userID); err != nil {
		return "", err
	}
	if o.settle != nil {
		if o.settle.total.Cmp(cmd.total) != 0 {
			return "", errParameterConflict
		}
		return o.settle.result, nil
	}
	if o.voided {
		return "", errInvalidOrder
	}
	o.userID = cmd.userID
	o.settle = &settle{result: result}
	o.settle.total.Set(cmd.total)
	return result, nil
}

func (o *operation) applyVoid() (string, error) {
	if o.voided {
		return Success, nil
	}
	if o.settle != nil {
		return "", errInvalidOrder
	}
	o.voided = true
	return Success, nil
}

func (o *operation) status(now func() time.Time) (string, error) {
	switch {
	case o.settle != nil && o.settle.result == Success:
		return statusPaid, nil
	case o.settle != nil:
		return statusDebt, nil
	case o.voided:
		return statusVoided, nil
	case o.hold != nil && o.hold.result == Failed:
		return statusHoldFailed, nil
	case o.hold != nil && !now().Before(o.hold.expiresAt):
		return statusHoldExpired, nil
	case o.hold != nil:
		return statusHeld, nil
	default:
		return "", errNotFound
	}
}
