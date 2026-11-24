package dynconfig

import (
	"errors"
	"maps"
	"strings"
	"time"
)

// defaultRetentionDays is the base retention period of a closed order.
const defaultRetentionDays = 365

// Retention decides when a closed order may be deleted.
type Retention struct {
	defaultDays int
	daysByZone  map[string]int
}

func NewRetention(defaultDays int, daysByZone map[string]int) (Retention, error) {
	if defaultDays <= 0 {
		return Retention{}, errors.New("default retention days must be positive")
	}
	for zone, days := range daysByZone {
		if strings.TrimSpace(zone) == "" || days <= 0 {
			return Retention{}, errors.New("zone retention requires a zone ID and positive days")
		}
	}
	return Retention{defaultDays: defaultDays, daysByZone: maps.Clone(daysByZone)}, nil
}

// DeleteAfter is closed_at + retention of the order's zone.
func (r Retention) DeleteAfter(closedAt time.Time, zoneID string) time.Time {
	days, ok := r.daysByZone[zoneID]
	if !ok {
		days = r.defaultDays
	}
	return closedAt.UTC().AddDate(0, 0, days)
}

func (r Retention) DefaultDays() int { return r.defaultDays }

func (r Retention) DaysByZone() map[string]int { return maps.Clone(r.daysByZone) }
