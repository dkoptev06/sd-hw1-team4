package catalog

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

type scooter struct {
	id     string
	zoneID string
	charge int
}

func newScooter(id, zoneID string, charge int) (scooter, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(zoneID) == "" {
		return scooter{}, errors.New("scooter and zone IDs are required")
	}
	if charge < 0 || charge > 100 {
		return scooter{}, fmt.Errorf("scooter %q has charge %d outside 0..100", id, charge)
	}
	return scooter{id: id, zoneID: zoneID, charge: charge}, nil
}

type tariffZone struct {
	id             string
	pricePerMinute int64
	priceUnlock    int64
	defaultDeposit int64
}

func newTariffZone(id string, pricePerMinute, priceUnlock, defaultDeposit int64) (tariffZone, error) {
	if strings.TrimSpace(id) == "" {
		return tariffZone{}, errors.New("tariff zone ID is required")
	}
	if pricePerMinute < 0 || priceUnlock < 0 || defaultDeposit < 0 {
		return tariffZone{}, fmt.Errorf("tariff zone %q has a negative amount", id)
	}
	return tariffZone{id: id, pricePerMinute: pricePerMinute, priceUnlock: priceUnlock, defaultDeposit: defaultDeposit}, nil
}

type userProfile struct {
	id              string
	hasSubscription bool
	trusted         bool
}

func newUserProfile(id string, hasSubscription, trusted bool) (userProfile, error) {
	if strings.TrimSpace(id) == "" {
		return userProfile{}, errors.New("user ID is required")
	}
	return userProfile{id: id, hasSubscription: hasSubscription, trusted: trusted}, nil
}

type configs struct {
	surge                     float64
	lowChargeDiscount         float64
	defaultRetentionDays      int
	retentionDaysByZone       map[string]int
	tariffZoneCacheTTLMinutes int
}

func newConfigs(surge, lowChargeDiscount float64, defaultDays int, daysByZone map[string]int, ttlMinutes int) (configs, error) {
	if !(surge > 0) || !(lowChargeDiscount > 0) || math.IsInf(surge, 0) || math.IsInf(lowChargeDiscount, 0) {
		return configs{}, errors.New("price coefficients must be finite and positive")
	}
	if defaultDays <= 0 || ttlMinutes <= 0 {
		return configs{}, errors.New("default retention and tariff cache TTL must be positive")
	}
	retention := make(map[string]int, len(daysByZone))
	for zone, days := range daysByZone {
		if strings.TrimSpace(zone) == "" || days <= 0 {
			return configs{}, fmt.Errorf("invalid retention for zone %q", zone)
		}
		retention[zone] = days
	}
	return configs{
		surge: surge, lowChargeDiscount: lowChargeDiscount, defaultRetentionDays: defaultDays,
		retentionDaysByZone: retention, tariffZoneCacheTTLMinutes: ttlMinutes,
	}, nil
}
