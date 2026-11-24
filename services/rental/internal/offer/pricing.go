package offer

import (
	"errors"
	"math/big"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

const lowChargeThreshold = 28

// isLowCharge is shared by pricing and the offer freshness check at start.
func isLowCharge(charge int) bool {
	return charge < lowChargeThreshold
}

// price calculates offer terms by the ADR formulas.
func price(tariff source.Tariff, user source.UserProfile, scooter source.Scooter, coeffs dynconfig.Coefficients) (Terms, error) {
	rate := floorMul(big.NewInt(tariff.PricePerMinute), coeffs.Surge)
	if isLowCharge(scooter.Charge) {
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

func floorMul(value *big.Int, factor *big.Rat) *big.Int {
	numerator := new(big.Int).Mul(value, factor.Num())
	// Both operands are non-negative, so truncating division is the floor.
	return numerator.Quo(numerator, factor.Denom())
}

// Terms are the money terms fixed in an offer and copied into its order.
type Terms struct {
	MinuteRate int64
	UnlockFee  int64
	Deposit    int64
}
