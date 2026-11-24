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
func Price(tariff Tariff, user UserProfile, scooter Scooter, coeffs Coefficients) (Terms, error) {
	rate := floorMul(big.NewInt(tariff.PricePerMinute), coeffs.Surge)
	if IsLowCharge(scooter.Charge) {
		rate = floorMul(rate, coeffs.LowChargeDiscount)
	}
	if !rate.IsInt64() {
		return Terms{}, errors.New("minute rate exceeds the supported amount")
	}
	terms := Terms{MinuteRate: rate.Int64(), UnlockFee: tariff.PriceUnlock, Deposit: tariff.DefaultDeposit}
	if user.HasSubscription {
		terms.UnlockFee = 0
	}
	if user.Trusted {
		terms.Deposit = 0
	}
	return terms, nil
}

// TripCost is zero for trips shorter than MinBillableDuration and otherwise
// unlock_fee + floor(floor(seconds) × minute_rate / 60).
func TripCost(terms Terms, startedAt, now time.Time) (*big.Int, error) {
	// Compute whole seconds directly: the difference between database timestamps
	// can exceed the range of time.Duration.
	seconds := now.Unix() - startedAt.Unix()
	if now.Nanosecond() < startedAt.Nanosecond() {
		seconds--
	}
	if seconds < int64(MinBillableDuration/time.Second) {
		return new(big.Int), nil
	}
	if terms.MinuteRate < 0 || terms.UnlockFee < 0 {
		return nil, errors.New("trip price terms must be non-negative")
	}

	cost := big.NewInt(seconds)
	cost.Mul(cost, big.NewInt(terms.MinuteRate))
	cost.Quo(cost, big.NewInt(int64(time.Minute/time.Second)))
	return cost.Add(cost, big.NewInt(terms.UnlockFee)), nil
}

func floorMul(value *big.Int, factor *big.Rat) *big.Int {
	numerator := new(big.Int).Mul(value, factor.Num())
	// Both operands are non-negative, so truncating division is the floor.
	return numerator.Quo(numerator, factor.Denom())
}
