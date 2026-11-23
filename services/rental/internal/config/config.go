// Package config defines Rental's settings and cold-start defaults.
package config

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"
)

// Provider supplies the current immutable configuration snapshot.
type Provider interface {
	Current() Config
}

type Config struct {
	surge                     json.Number
	lowChargeDiscount         json.Number
	retention                 Retention
	tariffZoneCacheTTLMinutes int
}

func New(surge, lowChargeDiscount json.Number, retention Retention, tariffZoneCacheTTLMinutes int) (Config, error) {
	if !positiveNumber(surge) || !positiveNumber(lowChargeDiscount) {
		return Config{}, errors.New("price coefficients must be positive JSON numbers")
	}
	if retention.defaultDays <= 0 || tariffZoneCacheTTLMinutes <= 0 {
		return Config{}, errors.New("retention and tariff cache TTL must be positive")
	}
	return Config{
		surge: surge, lowChargeDiscount: lowChargeDiscount,
		retention: retention, tariffZoneCacheTTLMinutes: tariffZoneCacheTTLMinutes,
	}, nil
}

func positiveNumber(value json.Number) bool {
	if !json.Valid([]byte(value.String())) {
		return false
	}
	number, ok := new(big.Rat).SetString(value.String())
	return ok && number.Sign() > 0
}

func (c Config) Surge() json.Number             { return c.surge }
func (c Config) LowChargeDiscount() json.Number { return c.lowChargeDiscount }
func (c Config) Retention() Retention           { return c.retention }
func (c Config) TariffZoneCacheTTLMinutes() int { return c.tariffZoneCacheTTLMinutes }

var defaults = func() Config {
	retention, err := NewRetention(365, nil)
	if err != nil {
		panic(err)
	}
	cfg, err := New("1.25", "0.8", retention, 10)
	if err != nil {
		panic(err)
	}
	return cfg
}()

func Default() Config { return defaults }

type Retention struct {
	defaultDays int
	daysByZone  map[string]int
}

func NewRetention(defaultDays int, daysByZone map[string]int) (Retention, error) {
	if defaultDays <= 0 {
		return Retention{}, errors.New("default retention days must be positive")
	}
	overrides := make(map[string]int, len(daysByZone))
	for zone, days := range daysByZone {
		if strings.TrimSpace(zone) == "" || days <= 0 {
			return Retention{}, errors.New("zone retention requires a zone and positive days")
		}
		overrides[zone] = days
	}
	return Retention{defaultDays: defaultDays, daysByZone: overrides}, nil
}

func (r Retention) DeleteAfter(closed time.Time, zoneID string) time.Time {
	days, configured := r.daysByZone[zoneID]
	if !configured {
		days = r.defaultDays
	}
	return closed.UTC().AddDate(0, 0, days)
}
