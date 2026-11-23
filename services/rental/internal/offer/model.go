// Package offer owns priced quotes and their use when starting a trip.
package offer

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNotFound          = errors.New("NOT_FOUND")
	ErrExpired           = errors.New("OFFER_EXPIRED")
	ErrChanged           = errors.New("OFFER_CHANGED")
	ErrParameterConflict = errors.New("PARAMETER_CONFLICT")
)

const ttl = 2 * time.Minute

type ScooterSnapshot struct {
	id     string
	zoneID string
	charge int
}

func NewScooterSnapshot(id, zoneID string, charge int) (ScooterSnapshot, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(zoneID) == "" {
		return ScooterSnapshot{}, errors.New("scooter and zone IDs are required")
	}
	if charge < 0 || charge > 100 {
		return ScooterSnapshot{}, errors.New("scooter charge must be between 0 and 100")
	}
	return ScooterSnapshot{id: id, zoneID: zoneID, charge: charge}, nil
}

func (s ScooterSnapshot) ID() string { return s.id }

type terms struct {
	scooter    ScooterSnapshot
	userID     string
	minuteRate int64
	unlockFee  int64
	deposit    int64
}

func newTerms(scooter ScooterSnapshot, userID string, minuteRate, unlockFee, deposit int64) (terms, error) {
	if scooter.id == "" || strings.TrimSpace(userID) == "" {
		return terms{}, errors.New("a scooter snapshot and user ID are required")
	}
	if minuteRate < 0 || unlockFee < 0 || deposit < 0 {
		return terms{}, errors.New("offer amounts cannot be negative")
	}
	return terms{scooter: scooter, userID: userID, minuteRate: minuteRate, unlockFee: unlockFee, deposit: deposit}, nil
}

type Quote struct {
	terms
	idempotencyID       string
	profileFallbackUsed bool
}

func NewQuote(scooter ScooterSnapshot, userID, idempotencyID string, minuteRate, unlockFee, deposit int64, profileFallbackUsed bool) (Quote, error) {
	if strings.TrimSpace(idempotencyID) == "" {
		return Quote{}, errors.New("idempotency ID is required")
	}
	priced, err := newTerms(scooter, userID, minuteRate, unlockFee, deposit)
	if err != nil {
		return Quote{}, err
	}
	return Quote{terms: priced, idempotencyID: idempotencyID, profileFallbackUsed: profileFallbackUsed}, nil
}

type Receipt struct {
	OfferID    uuid.UUID
	MinuteRate int64
	UnlockFee  int64
	Deposit    int64
	ExpiresAt  time.Time
	Replayed   bool
}

// StartSnapshot is the locked offer data shared with the order use case.
type StartSnapshot struct {
	terms
	status    string
	expiresAt time.Time
}

func newStartSnapshot(scooter ScooterSnapshot, userID string, minuteRate, unlockFee, deposit int64, status string, expiresAt time.Time) (StartSnapshot, error) {
	if (status != "AVAILABLE" && status != "CONSUMED") || expiresAt.IsZero() {
		return StartSnapshot{}, errors.New("offer status or expiration is invalid")
	}
	priced, err := newTerms(scooter, userID, minuteRate, unlockFee, deposit)
	if err != nil {
		return StartSnapshot{}, err
	}
	return StartSnapshot{terms: priced, status: status, expiresAt: expiresAt.UTC()}, nil
}

func (o StartSnapshot) ScooterID() string { return o.scooter.id }
func (o StartSnapshot) ZoneID() string    { return o.scooter.zoneID }
func (o StartSnapshot) UserID() string    { return o.userID }
func (o StartSnapshot) MinuteRate() int64 { return o.minuteRate }
func (o StartSnapshot) UnlockFee() int64  { return o.unlockFee }
func (o StartSnapshot) Deposit() int64    { return o.deposit }

// CanStart checks the offer against the current scooter data and database time.
func (o StartSnapshot) CanStart(now time.Time, current ScooterSnapshot) error {
	if err := checkAvailability(o.status, o.expiresAt, now); err != nil {
		return err
	}
	if current.id != o.scooter.id || current.zoneID != o.scooter.zoneID ||
		IsLowCharge(current.charge) != IsLowCharge(o.scooter.charge) {
		return ErrChanged
	}
	return nil
}

func checkAvailability(status string, expiresAt, now time.Time) error {
	if status != "AVAILABLE" || !now.Before(expiresAt) {
		return ErrExpired
	}
	return nil
}

// IsLowCharge is shared by quote pricing and offer freshness validation.
func IsLowCharge(charge int) bool {
	return charge < 28
}
