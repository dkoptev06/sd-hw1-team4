package domain

import (
	"time"

	"github.com/google/uuid"
)

type OfferStatus string

const (
	OfferAvailable OfferStatus = "AVAILABLE"
	OfferConsumed  OfferStatus = "CONSUMED"
)

// Offer fixes money terms for one user and scooter for a limited time.
type Offer struct {
	ID              uuid.UUID
	IdempotencyID   string
	UserID          string
	Scooter         Scooter
	Terms           Terms
	ProfileFallback bool
	Status          OfferStatus
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// MatchesRequest reports whether a replayed creation request has the same parameters.
func (o Offer) MatchesRequest(userID, scooterID string) bool {
	return o.UserID == userID && o.Scooter.ID == scooterID
}

// CheckStart validates that the offer can create an order now, given fresh scooter data.
// The zone and the low-charge category must not change; the exact charge may.
func (o Offer) CheckStart(now time.Time, current Scooter) error {
	if o.Status != OfferAvailable || !now.Before(o.ExpiresAt) {
		return ErrOfferExpired
	}
	if current.ID != o.Scooter.ID || current.ZoneID != o.Scooter.ZoneID ||
		IsLowCharge(current.Charge) != IsLowCharge(o.Scooter.Charge) {
		return ErrOfferChanged
	}
	return nil
}
