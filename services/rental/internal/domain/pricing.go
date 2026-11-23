package domain

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const (
	// LowChargeThreshold is the charge below which the low-charge discount applies.
	LowChargeThreshold = 28
	// MinBillableDuration is the shortest trip that is charged.
	MinBillableDuration = 5 * time.Second
)

// Scooter is the scooter data Rental reads from Source.
type Scooter struct {
	ID     string
	ZoneID string
	Charge int
}

func (s Scooter) Validate() error {
	if strings.TrimSpace(s.ID) == "" || strings.TrimSpace(s.ZoneID) == "" {
		return errors.New("scooter and zone IDs are required")
	}
	if s.Charge < 0 || s.Charge > 100 {
		return fmt.Errorf("scooter charge %d is outside 0..100", s.Charge)
	}
	return nil
}

// Tariff is the price list of a tariff zone.
type Tariff struct {
	ZoneID         string
	PricePerMinute int64
	PriceUnlock    int64
	DefaultDeposit int64
}

func (t Tariff) Validate() error {
	if strings.TrimSpace(t.ZoneID) == "" {
		return errors.New("tariff zone ID is required")
	}
	if t.PricePerMinute < 0 || t.PriceUnlock < 0 || t.DefaultDeposit < 0 {
		return errors.New("tariff amounts cannot be negative")
	}
	return nil
}

// UserProfile holds the pricing-relevant user flags. Fallback marks a profile
// substituted because the users source was unavailable.
type UserProfile struct {
	UserID          string
	HasSubscription bool
	Trusted         bool
	Fallback        bool
}

// FallbackProfile is the greedy pricing profile used when users is unavailable.
func FallbackProfile(userID string) UserProfile {
	return UserProfile{UserID: userID, Fallback: true}
}

// Coefficients are the price multipliers from the configs source, kept as exact rationals.
type Coefficients struct {
	Surge             *big.Rat
	LowChargeDiscount *big.Rat
}

// ParseCoefficient parses a positive decimal number such as "1.25".
func ParseCoefficient(value string) (*big.Rat, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "/") {
		return nil, fmt.Errorf("invalid coefficient %q", value)
	}
	r, ok := new(big.Rat).SetString(value)
	if !ok || r.Sign() <= 0 {
		return nil, fmt.Errorf("coefficient %q must be a positive number", value)
	}
	return r, nil
}

// Terms are the money terms fixed in an offer and copied into its order.
type Terms struct {
	MinuteRate int64
	UnlockFee  int64
	Deposit    int64
}

// IsLowCharge is shared by pricing and the offer freshness check at start.
func IsLowCharge(charge int) bool {
	return charge < LowChargeThreshold
}

// Price calculates offer terms by the ADR formulas.
func Price(tariff Tariff, user UserProfile, scooter Scooter, coeffs Coefficients) Terms {
	rate := floorMul(tariff.PricePerMinute, coeffs.Surge)
	if IsLowCharge(scooter.Charge) {
		rate = floorMul(rate, coeffs.LowChargeDiscount)
	}
	terms := Terms{MinuteRate: rate, UnlockFee: tariff.PriceUnlock, Deposit: tariff.DefaultDeposit}
	if user.HasSubscription {
		terms.UnlockFee = 0
	}
	if user.Trusted {
		terms.Deposit = 0
	}
	return terms
}

// TripCost is zero for trips shorter than MinBillableDuration and otherwise
// unlock_fee + floor(floor(seconds) × minute_rate / 60).
func TripCost(terms Terms, elapsed time.Duration) int64 {
	if elapsed < MinBillableDuration {
		return 0
	}
	seconds := int64(elapsed / time.Second)
	return terms.UnlockFee + seconds*terms.MinuteRate/60
}

func floorMul(value int64, factor *big.Rat) int64 {
	product := new(big.Rat).Mul(new(big.Rat).SetInt64(value), factor)
	// Both operands are non-negative, so truncating division is the floor.
	return new(big.Int).Quo(product.Num(), product.Denom()).Int64()
}
