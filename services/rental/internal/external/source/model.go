package source

import (
	"errors"
	"fmt"
	"strings"
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

func (t Tariff) validate() error {
	if strings.TrimSpace(t.ZoneID) == "" {
		return errors.New("tariff zone ID is required")
	}
	if t.PricePerMinute < 0 || t.PriceUnlock < 0 || t.DefaultDeposit < 0 {
		return errors.New("tariff amounts cannot be negative")
	}
	return nil
}

// UserProfile holds the pricing-relevant flags read from Source.
type UserProfile struct {
	UserID          string
	HasSubscription bool
	Trusted         bool
}
