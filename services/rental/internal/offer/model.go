// Package offer owns priced quotes and their use when starting a trip.
package offer

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

type quoteRequest struct {
	scooterID     string
	userID        string
	idempotencyID string
}

func newQuoteRequest(scooterID, userID, idempotencyID string) (quoteRequest, error) {
	if strings.TrimSpace(scooterID) == "" || strings.TrimSpace(userID) == "" || strings.TrimSpace(idempotencyID) == "" {
		return quoteRequest{}, fmt.Errorf("%w: scooter_id, user_id and idempotency_id are required", ErrInvalidRequest)
	}
	return quoteRequest{scooterID: scooterID, userID: userID, idempotencyID: idempotencyID}, nil
}

type quote struct {
	id              uuid.UUID
	request         quoteRequest
	scooter         source.Scooter
	terms           Terms
	profileFallback bool
}

func newQuote(id uuid.UUID, request quoteRequest, scooter source.Scooter, terms Terms, fallback bool) (quote, error) {
	if id == uuid.Nil || scooter.ID != request.scooterID {
		return quote{}, errors.New("invalid offer ID or scooter snapshot")
	}
	if err := scooter.Validate(); err != nil {
		return quote{}, err
	}
	if terms.MinuteRate < 0 || terms.UnlockFee < 0 || terms.Deposit < 0 {
		return quote{}, errors.New("offer amounts cannot be negative")
	}
	return quote{id: id, request: request, scooter: scooter, terms: terms, profileFallback: fallback}, nil
}

// pricingProfile records whether offer pricing used the conservative fallback.
type pricingProfile struct {
	source.UserProfile
	fallback bool
}

func fallbackProfile(userID string) pricingProfile {
	return pricingProfile{UserProfile: source.UserProfile{UserID: userID}, fallback: true}
}

func priceQuote(id uuid.UUID, request quoteRequest, scooter source.Scooter, tariff source.Tariff, profile pricingProfile, coeffs dynconfig.Coefficients) (quote, error) {
	terms, err := price(tariff, profile.UserProfile, scooter, coeffs)
	if err != nil {
		return quote{}, err
	}
	return newQuote(id, request, scooter, terms, profile.fallback)
}

type status string

const (
	available status = "AVAILABLE"
	consumed  status = "CONSUMED"
)

type storedOffer struct {
	quote
	status    status
	createdAt time.Time
	expiresAt time.Time
}

func restoreOffer(q quote, state status, createdAt, expiresAt time.Time) (storedOffer, error) {
	if state != available && state != consumed {
		return storedOffer{}, errors.New("invalid offer status")
	}
	if createdAt.IsZero() || expiresAt.IsZero() {
		return storedOffer{}, errors.New("offer creation and expiration times are required")
	}
	return storedOffer{quote: q, status: state, createdAt: createdAt.UTC(), expiresAt: expiresAt.UTC()}, nil
}

func (o storedOffer) checkReplay(request quoteRequest) error {
	if o.request.userID != request.userID || o.request.scooterID != request.scooterID {
		return fmt.Errorf("%w: idempotency_id %q was used with other parameters", ErrParameterConflict, request.idempotencyID)
	}
	return nil
}

func (o storedOffer) checkAvailable() error {
	if o.status != available {
		return ErrOfferExpired
	}
	return nil
}

func (o storedOffer) checkStart(now time.Time, current source.Scooter) error {
	if err := o.checkAvailable(); err != nil {
		return err
	}
	if !now.Before(o.expiresAt) {
		return ErrOfferExpired
	}
	if current.ID != o.scooter.ID || current.ZoneID != o.scooter.ZoneID ||
		isLowCharge(current.Charge) != isLowCharge(o.scooter.Charge) {
		return ErrOfferChanged
	}
	return nil
}
