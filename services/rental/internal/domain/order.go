package domain

import (
	"math/big"
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	OrderStarting    OrderStatus = "STARTING"
	OrderActive      OrderStatus = "ACTIVE"
	OrderStartFailed OrderStatus = "START_FAILED"
	OrderCompleted   OrderStatus = "COMPLETED"
	OrderCancelled   OrderStatus = "CANCELLED"
)

// IsFinal reports whether the order reached a terminal state.
func (s OrderStatus) IsFinal() bool {
	return s == OrderStartFailed || s == OrderCompleted || s == OrderCancelled
}

// IsFinished reports whether the trip was finished by the client.
func (s OrderStatus) IsFinished() bool {
	return s == OrderCompleted || s == OrderCancelled
}

// Order is a rental trip created from an offer.
type Order struct {
	ID          uuid.UUID
	OfferID     uuid.UUID
	UserID      string
	ScooterID   string
	ZoneID      string
	Terms       Terms
	Status      OrderStatus
	CreatedAt   time.Time
	StartedAt   *time.Time
	FinishTime  *time.Time
	ClosedAt    *time.Time
	DeleteAfter *time.Time
	Cost        *big.Int
}

// NewOrder creates a STARTING order with the offer's terms.
func NewOrder(offer Offer, now time.Time) Order {
	return Order{
		ID:        uuid.New(),
		OfferID:   offer.ID,
		UserID:    offer.UserID,
		ScooterID: offer.Scooter.ID,
		ZoneID:    offer.Scooter.ZoneID,
		Terms:     offer.Terms,
		Status:    OrderStarting,
		CreatedAt: now,
	}
}

// CostAt is the cost shown to the client: undefined while STARTING, calculated
// on read while ACTIVE and fixed afterwards.
func (o Order) CostAt(now time.Time) (*big.Int, error) {
	if o.Status == OrderActive && o.StartedAt != nil {
		return TripCost(o.Terms, *o.StartedAt, now)
	}
	return o.Cost, nil
}

// CanBeReplacedAt reports whether a new start may close this order because its start deadline passed.
func (o Order) CanBeReplacedAt(now time.Time, startDeadline time.Time) bool {
	return o.Status == OrderStarting && !now.Before(startDeadline)
}

// FinishOutcome calculates the final status and cost of an ACTIVE trip.
func FinishOutcome(terms Terms, startedAt, finishedAt time.Time) (OrderStatus, *big.Int, error) {
	if finishedAt.Before(startedAt.Add(MinBillableDuration)) {
		return OrderCancelled, new(big.Int), nil
	}
	cost, err := TripCost(terms, startedAt, finishedAt)
	if err != nil {
		return "", nil, err
	}
	return OrderCompleted, cost, nil
}
