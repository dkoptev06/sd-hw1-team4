package order

import (
	"errors"
	"math/big"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
)

// minBillableDuration is the shortest trip that is charged.
const minBillableDuration = 5 * time.Second

// tripCost is zero for trips shorter than minBillableDuration and otherwise
// unlock_fee + floor(floor(seconds) × minute_rate / 60).
func tripCost(terms offer.Terms, startedAt, now time.Time) (*big.Int, error) {
	// Compute whole seconds directly: the difference between database timestamps
	// can exceed the range of time.Duration.
	seconds := now.Unix() - startedAt.Unix()
	if now.Nanosecond() < startedAt.Nanosecond() {
		seconds--
	}
	if seconds < int64(minBillableDuration/time.Second) {
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

// finishOutcome calculates the final status and cost of an ACTIVE trip.
func finishOutcome(terms offer.Terms, startedAt, finishedAt time.Time) (Status, *big.Int, error) {
	if finishedAt.Before(startedAt.Add(minBillableDuration)) {
		return Cancelled, new(big.Int), nil
	}
	cost, err := tripCost(terms, startedAt, finishedAt)
	if err != nil {
		return "", nil, err
	}
	return Completed, cost, nil
}
