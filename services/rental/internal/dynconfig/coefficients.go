package dynconfig

import (
	"fmt"
	"math/big"
	"strings"
)

// Coefficients are the price multipliers from the configs source, kept as exact rationals.
type Coefficients struct {
	Surge             *big.Rat
	LowChargeDiscount *big.Rat
}

// parseCoefficient parses a positive decimal number such as "1.25".
func parseCoefficient(value string) (*big.Rat, error) {
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
