// Package order owns the lifecycle and price of a rental trip.
package order

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
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

func (s Status) isFinal() bool {
	return s == StartFailed || s == Completed || s == Cancelled
}

type state struct {
	status      Status
	createdAt   time.Time
	startedAt   *time.Time
	finishTime  *time.Time
	closedAt    *time.Time
	deleteAfter *time.Time
	cost        *big.Int
}

type trip struct {
	id, offerID               uuid.UUID
	userID, scooterID, zoneID string
	terms                     offer.Terms
	state
}

func newTrip(offerID uuid.UUID, userID string, scooter source.Scooter, terms offer.Terms, now time.Time) (trip, error) {
	return restoreTrip(uuid.New(), offerID, userID, scooter.ID, scooter.ZoneID, terms, state{status: Starting, createdAt: now})
}

func restoreTrip(id, offerID uuid.UUID, userID, scooterID, zoneID string, terms offer.Terms, state state) (trip, error) {
	if id == uuid.Nil || offerID == uuid.Nil || strings.TrimSpace(userID) == "" ||
		strings.TrimSpace(scooterID) == "" || strings.TrimSpace(zoneID) == "" {
		return trip{}, errors.New("order requires IDs for the order, offer, user, scooter and zone")
	}
	switch state.status {
	case Starting, Active, StartFailed, Completed, Cancelled:
	default:
		return trip{}, fmt.Errorf("unknown order status %q", state.status)
	}
	if state.createdAt.IsZero() || terms.MinuteRate < 0 || terms.UnlockFee < 0 || terms.Deposit < 0 ||
		(state.cost != nil && state.cost.Sign() < 0) {
		return trip{}, errors.New("invalid order time or money terms")
	}
	return trip{id: id, offerID: offerID, userID: userID, scooterID: scooterID, zoneID: zoneID, terms: terms, state: state}, nil
}

func (o trip) costAt(now time.Time) (*big.Int, error) {
	if o.status == Active && o.startedAt != nil {
		return tripCost(o.terms, *o.startedAt, now)
	}
	return copyCost(o.cost), nil
}

func copyCost(cost *big.Int) *big.Int {
	if cost == nil {
		return nil
	}
	return new(big.Int).Set(cost)
}

func (o trip) canBeReplacedAt(now time.Time, deadline *time.Time) bool {
	return o.status == Starting && deadline != nil && !now.Before(*deadline)
}

func (o trip) finishRequired() (bool, error) {
	switch o.status {
	case Completed, Cancelled:
		return false, nil
	case Active:
		if o.startedAt == nil {
			return false, fmt.Errorf("active order %s has no started_at", o.id)
		}
		return true, nil
	default:
		return false, fmt.Errorf("%w: order %s is %s", ErrOrderNotActive, o.id, o.status)
	}
}

func (o trip) finish(now time.Time, retention dynconfig.Retention) (trip, error) {
	status, cost, err := finishOutcome(o.terms, *o.startedAt, now)
	if err != nil {
		return trip{}, fmt.Errorf("finish order %s: %w", o.id, err)
	}
	o.status, o.cost = status, cost
	o.finishTime, o.closedAt = &now, &now
	deleteAfter := retention.DeleteAfter(now, o.zoneID)
	o.deleteAfter = &deleteAfter
	return o, nil
}

func (o trip) closeStart(now time.Time, retention dynconfig.Retention) trip {
	o.status = StartFailed
	o.closedAt = &now
	deleteAfter := retention.DeleteAfter(now, o.zoneID)
	o.deleteAfter = &deleteAfter
	o.cost = new(big.Int)
	return o
}

func (o trip) acceptsStartResult() bool { return o.status == Starting }

func (o trip) startFailureReason(reason task.Reason) task.Reason {
	if reason == task.MaxAttemptsExceeded && o.status == StartFailed {
		return task.DeadlineExceeded
	}
	return reason
}

func (o trip) acceptsStartFinalization() bool {
	return o.status == Starting || o.status == StartFailed
}

func (o trip) needsVoid(reason task.Reason) bool {
	return o.terms.Deposit > 0 && reason != task.PaymentRejected
}

type startDecision struct {
	activate bool
	retry    bool
	reason   task.Reason
}

func (o trip) decideStart(outcome task.Outcome) startDecision {
	switch outcome {
	case task.Success:
		return startDecision{activate: true}
	case task.Rejected:
		return startDecision{reason: task.PaymentRejected}
	case task.ContractError:
		return startDecision{reason: task.ContractViolation}
	default:
		return startDecision{retry: true}
	}
}
