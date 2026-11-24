// Package dynconfig keeps the latest valid snapshot of the Source /configs data.
package dynconfig

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
)

// Snapshot is an immutable validated configuration.
type Snapshot struct {
	Coefficients domain.Coefficients
	Retention    domain.Retention
	TariffTTL    time.Duration
}

const defaultTariffTTLMinutes = 10

// Defaults are the cold-start values used until Source returns a valid config.
func Defaults() Snapshot {
	snapshot, err := Parse(defaultConfigs())
	if err != nil {
		panic(err)
	}
	return snapshot
}

func defaultConfigs() source.Configs {
	var raw source.Configs
	raw.PriceCoeffSettings.Surge = "1.25"
	raw.PriceCoeffSettings.LowChargeDiscount = "0.8"
	raw.DefaultRetentionDays = domain.DefaultRetentionDays
	raw.RetentionDaysByZone = map[string]int{}
	return raw
}

// Parse validates a raw /configs response. A missing TTL takes the default of 10 minutes.
func Parse(raw source.Configs) (Snapshot, error) {
	surge, err := domain.ParseCoefficient(raw.PriceCoeffSettings.Surge.String())
	if err != nil {
		return Snapshot{}, fmt.Errorf("surge: %w", err)
	}
	discount, err := domain.ParseCoefficient(raw.PriceCoeffSettings.LowChargeDiscount.String())
	if err != nil {
		return Snapshot{}, fmt.Errorf("low_charge_discount: %w", err)
	}
	if raw.RetentionDaysByZone == nil {
		return Snapshot{}, errors.New("retention_days_by_zone must be an object")
	}
	retention, err := domain.NewRetention(raw.DefaultRetentionDays, raw.RetentionDaysByZone)
	if err != nil {
		return Snapshot{}, err
	}
	ttlMinutes := defaultTariffTTLMinutes
	if raw.TariffZoneCacheTTLMinutes != nil {
		ttlMinutes = *raw.TariffZoneCacheTTLMinutes
	}
	if ttlMinutes <= 0 {
		return Snapshot{}, errors.New("tariff_zone_cache_ttl_minutes must be positive")
	}
	const maxTariffTTLMinutes = int(math.MaxInt64 / time.Minute)
	if ttlMinutes > maxTariffTTLMinutes {
		return Snapshot{}, fmt.Errorf("tariff_zone_cache_ttl_minutes must not exceed %d", maxTariffTTLMinutes)
	}
	return Snapshot{
		Coefficients: domain.Coefficients{Surge: surge, LowChargeDiscount: discount},
		Retention:    retention,
		TariffTTL:    time.Duration(ttlMinutes) * time.Minute,
	}, nil
}

// Fetcher reads the raw config from Source.
type Fetcher interface {
	Configs(ctx context.Context) (source.Configs, error)
}

// Provider serves the current snapshot and refreshes it periodically. On a failed
// or invalid refresh it keeps the last valid snapshot, or the defaults on cold start.
type Provider struct {
	fetcher  Fetcher
	interval time.Duration
	log      *slog.Logger
	current  atomic.Pointer[Snapshot]
}

func NewProvider(fetcher Fetcher, interval time.Duration, log *slog.Logger) *Provider {
	p := &Provider{fetcher: fetcher, interval: interval, log: log}
	defaults := Defaults()
	p.current.Store(&defaults)
	metrics.ConfigUsingDefaults.Set(1)
	return p
}

func (p *Provider) Current() Snapshot { return *p.current.Load() }

// TariffTTL is a convenience accessor for the tariff cache.
func (p *Provider) TariffTTL() time.Duration { return p.Current().TariffTTL }

// Refresh fetches and installs a new snapshot once.
func (p *Provider) Refresh(ctx context.Context) error {
	raw, err := p.fetcher.Configs(ctx)
	if err != nil {
		metrics.ConfigRefreshes.WithLabelValues("unavailable").Inc()
		return err
	}
	snapshot, err := Parse(raw)
	if err != nil {
		metrics.ConfigRefreshes.WithLabelValues("invalid").Inc()
		return fmt.Errorf("invalid config: %w", err)
	}
	p.current.Store(&snapshot)
	metrics.ConfigRefreshes.WithLabelValues("ok").Inc()
	metrics.ConfigUsingDefaults.Set(0)
	return nil
}

// Start loads the config once (falling back to defaults on cold start) and then
// refreshes it in the background every interval until ctx is done.
func (p *Provider) Start(ctx context.Context) {
	p.refreshAndLog(ctx)
	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.refreshAndLog(ctx)
			}
		}
	}()
}

func (p *Provider) refreshAndLog(ctx context.Context) {
	if err := p.Refresh(ctx); err != nil && ctx.Err() == nil {
		p.log.Warn("config refresh failed; keeping the previous config", "error", err)
	}
}
