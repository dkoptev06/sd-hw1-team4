// Package catalog serves the immutable in-memory Source catalog.
package catalog

import (
	"errors"
	"fmt"
)

type Service struct {
	scooters map[string]scooter
	zones    map[string]tariffZone
	users    map[string]userProfile
	config   configs
}

// NewService seeds the four tariff zones and the pricing cases used by the load test.
func NewService(scooters, users int) (*Service, error) {
	if scooters < 0 || users < 0 {
		return nil, errors.New("catalog sizes must be non-negative")
	}

	config, err := newConfigs(1.25, 0.8, 365, nil, 10)
	if err != nil {
		return nil, err
	}
	tariffs := []struct{ perMinute, unlock, deposit int64 }{
		{10, 50, 300},
		{12, 40, 250},
		{8, 30, 200},
		{15, 60, 350},
	}
	c := &Service{
		scooters: make(map[string]scooter, scooters),
		zones:    make(map[string]tariffZone, len(tariffs)),
		users:    make(map[string]userProfile, users),
		config:   config,
	}
	for i, t := range tariffs {
		zone, err := newTariffZone(zoneID(i), t.perMinute, t.unlock, t.deposit)
		if err != nil {
			return nil, err
		}
		c.zones[zone.id] = zone
	}
	for i := 1; i <= scooters; i++ {
		value, err := newScooter(fmt.Sprintf("scooter-%d", i), zoneID(i%len(tariffs)), i*37%101)
		if err != nil {
			return nil, err
		}
		c.scooters[value.id] = value
	}
	for i := 1; i <= users; i++ {
		value, err := newUserProfile(fmt.Sprintf("user-%d", i), i%4 == 0, i%3 == 0)
		if err != nil {
			return nil, err
		}
		c.users[value.id] = value
	}
	return c, nil
}

func zoneID(i int) string { return fmt.Sprintf("zone-%d", i+1) }

func (c *Service) scooter(id string) (scooter, bool) {
	value, ok := c.scooters[id]
	return value, ok
}

func (c *Service) tariffZone(id string) (tariffZone, bool) {
	value, ok := c.zones[id]
	return value, ok
}

func (c *Service) userProfile(id string) (userProfile, bool) {
	value, ok := c.users[id]
	return value, ok
}
