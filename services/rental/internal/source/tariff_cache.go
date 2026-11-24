package source

import (
	"context"
	"errors"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
)

// TariffFetcher reads a tariff zone from Source.
type TariffFetcher func(ctx context.Context, zoneID string) (domain.Tariff, error)

type cachedTariff struct {
	tariff    domain.Tariff
	fetchedAt time.Time
}

// TariffCache is a read-through LRU cache. An entry older than the TTL is invalid:
// it is refreshed from Source and, if Source fails, the lookup fails with ZONE_STALE.
type TariffCache struct {
	fetch TariffFetcher
	ttl   func() time.Duration
	now   func() time.Time
	cache *lru.Cache[string, cachedTariff]
	group singleflight.Group
}

// NewTariffCache creates a cache of the given size. ttl is read on every lookup,
// so a config change applies immediately.
func NewTariffCache(size int, fetch TariffFetcher, ttl func() time.Duration, now func() time.Time) (*TariffCache, error) {
	cache, err := lru.New[string, cachedTariff](size)
	if err != nil {
		return nil, err
	}
	return &TariffCache{fetch: fetch, ttl: ttl, now: now, cache: cache}, nil
}

func (c *TariffCache) isFresh(entry cachedTariff) bool {
	return c.now().Sub(entry.fetchedAt) < c.ttl()
}

// Tariff returns a fresh tariff or a domain error: ZONE_STALE when only an expired
// entry is available, SOURCE_UNAVAILABLE when there is nothing cached.
func (c *TariffCache) Tariff(ctx context.Context, zoneID string) (domain.Tariff, error) {
	cached, found := c.cache.Get(zoneID)
	if found && c.isFresh(cached) {
		metrics.TariffCache.WithLabelValues("hit").Inc()
		return cached.tariff, nil
	}

	// The shared fetch must not depend on the caller that happened to start it: if
	// that request is cancelled, the other callers still get Source's answer. The
	// Source client bounds the call with its own timeout.
	fetchCtx := context.WithoutCancel(ctx)
	results := c.group.DoChan(zoneID, func() (any, error) {
		// A previous fetch may have completed since the caller looked in the cache.
		if current, ok := c.cache.Get(zoneID); ok && c.isFresh(current) {
			return current.tariff, nil
		}
		tariff, err := c.fetch(fetchCtx, zoneID)
		if err != nil {
			return nil, err
		}
		c.cache.Add(zoneID, cachedTariff{tariff: tariff, fetchedAt: c.now()})
		return tariff, nil
	})
	var result singleflight.Result
	select {
	case <-ctx.Done():
		return domain.Tariff{}, ctx.Err()
	case result = <-results:
	}
	if result.Err == nil {
		metrics.TariffCache.WithLabelValues("miss").Inc()
		return result.Val.(domain.Tariff), nil
	}
	err := result.Err

	metrics.TariffCache.WithLabelValues("error").Inc()
	switch {
	case found:
		return domain.Tariff{}, domain.Wrap(domain.ErrZoneStale, "tariff zone %q: %v", zoneID, err)
	case errors.Is(err, ErrNotFound):
		// The scooter refers to a zone Source does not know: Source data is inconsistent.
		return domain.Tariff{}, domain.Wrap(domain.ErrSourceUnavailable, "tariff zone %q not found", zoneID)
	default:
		return domain.Tariff{}, domain.Wrap(domain.ErrSourceUnavailable, "tariff zone %q: %v", zoneID, err)
	}
}
