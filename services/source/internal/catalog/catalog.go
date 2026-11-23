// Package catalog holds the in-memory data served by the Source stub.
package catalog

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Scooter struct {
	ID     string `json:"id"`
	ZoneID string `json:"zone_id"`
	Charge int    `json:"charge"`
}

type TariffZone struct {
	ID             string `json:"id"`
	PricePerMinute int64  `json:"price_per_minute"`
	PriceUnlock    int64  `json:"price_unlock"`
	DefaultDeposit int64  `json:"default_deposit"`
}

type UserProfile struct {
	ID              string `json:"id"`
	HasSubscription bool   `json:"has_subscription"`
	Trusted         bool   `json:"trusted"`
}

type PriceCoeffSettings struct {
	Surge             json.Number `json:"surge"`
	LowChargeDiscount json.Number `json:"low_charge_discount"`
}

type Configs struct {
	PriceCoeffSettings        PriceCoeffSettings `json:"price_coeff_settings"`
	DefaultRetentionDays      int                `json:"default_retention_days"`
	RetentionDaysByZone       map[string]int     `json:"retention_days_by_zone"`
	TariffZoneCacheTTLMinutes int                `json:"tariff_zone_cache_ttl_minutes"`
}

// Catalog is safe for concurrent use.
type Catalog struct {
	mu       sync.RWMutex
	scooters map[string]Scooter
	zones    map[string]TariffZone
	users    map[string]UserProfile
	configs  Configs
}

// Seed builds the data set of the load test plan: scooters spread evenly over
// four zones with charge on both sides of the low-charge threshold, and users
// with every combination of subscription and trust.
func Seed(scooters, users int) *Catalog {
	c := &Catalog{
		scooters: make(map[string]Scooter, scooters),
		zones:    make(map[string]TariffZone, 4),
		users:    make(map[string]UserProfile, users),
		configs: Configs{
			PriceCoeffSettings:        PriceCoeffSettings{Surge: "1.25", LowChargeDiscount: "0.8"},
			DefaultRetentionDays:      365,
			RetentionDaysByZone:       map[string]int{},
			TariffZoneCacheTTLMinutes: 10,
		},
	}
	tariffs := []struct{ perMinute, unlock, deposit int64 }{{10, 50, 300}, {12, 40, 250}, {8, 30, 200}, {15, 60, 350}}
	for i, t := range tariffs {
		id := zoneID(i)
		c.zones[id] = TariffZone{ID: id, PricePerMinute: t.perMinute, PriceUnlock: t.unlock, DefaultDeposit: t.deposit}
	}
	for i := 1; i <= scooters; i++ {
		id := fmt.Sprintf("scooter-%d", i)
		c.scooters[id] = Scooter{ID: id, ZoneID: zoneID(i % len(tariffs)), Charge: i * 37 % 101}
	}
	for i := 1; i <= users; i++ {
		id := fmt.Sprintf("user-%d", i)
		c.users[id] = UserProfile{ID: id, HasSubscription: i%4 == 0, Trusted: i%3 == 0}
	}
	return c
}

func zoneID(i int) string { return fmt.Sprintf("zone-%d", i+1) }

func (c *Catalog) Scooter(id string) (Scooter, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.scooters[id]
	return s, ok
}

func (c *Catalog) TariffZone(id string) (TariffZone, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	z, ok := c.zones[id]
	return z, ok
}

func (c *Catalog) UserProfile(id string) (UserProfile, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.users[id]
	return u, ok
}

func (c *Catalog) Configs() Configs {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.configs
}
