// Package ordercache caches immutable archived orders and bounds GET load on PostgreSQL.
package ordercache

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
)

type DBLoader func(context.Context, uuid.UUID) (domain.Order, bool, time.Time, error)

const (
	dbFallbackTimeout     = 2 * time.Second
	redisFailureThreshold = 3
	dbWaitersPerSlot      = 4
)

type cachedEntry struct {
	Order     domain.Order
	ExpiresAt time.Time
}

func (e cachedEntry) valid(id uuid.UUID, now time.Time) bool {
	return e.Order.ID == id && e.Order.Status.IsFinal() && e.Order.DeleteAfter != nil &&
		now.Before(e.ExpiresAt) && now.Before(*e.Order.DeleteAfter)
}

type loadResult struct {
	order    domain.Order
	archived bool
	now      time.Time
}

type OrderCache struct {
	cfg    settings.OrderCache
	l1     *lru.Cache[uuid.UUID, cachedEntry]
	remote RemoteCache
	load   DBLoader
	now    func() time.Time
	group  singleflight.Group

	redisMu         sync.Mutex
	redisGeneration uint64
	redisFailures   int
	redisRetryAt    time.Time
	redisProbing    bool

	dbMu       sync.Mutex
	dbInflight int
	dbWaiters  []chan struct{}
}

func New(cfg settings.OrderCache, remote RemoteCache, load DBLoader, now func() time.Time) (*OrderCache, error) {
	l1, err := lru.New[uuid.UUID, cachedEntry](cfg.Size)
	if err != nil {
		return nil, err
	}
	return &OrderCache{cfg: cfg, l1: l1, remote: remote, load: load, now: now}, nil
}

func (c *OrderCache) Close() error {
	if c.remote == nil {
		return nil
	}
	return c.remote.Close()
}

func (c *OrderCache) Get(ctx context.Context, id uuid.UUID) (domain.Order, bool, time.Time, error) {
	now := c.now().UTC()
	if entry, ok := c.l1.Get(id); ok && entry.valid(id, now) {
		metrics.OrderCacheLookups.WithLabelValues("l1", "hit").Inc()
		return entry.Order, true, now, nil
	}
	metrics.OrderCacheLookups.WithLabelValues("l1", "miss").Inc()
	if err := ctx.Err(); err != nil {
		return domain.Order{}, false, time.Time{}, err
	}

	ch := c.group.DoChan(id.String(), func() (any, error) {
		// One short-lived operation serves all waiters, even if its first caller leaves.
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dbFallbackTimeout)
		defer cancel()
		return c.loadThrough(loadCtx, id)
	})
	select {
	case <-ctx.Done():
		return domain.Order{}, false, time.Time{}, ctx.Err()
	case result := <-ch:
		if result.Shared {
			metrics.OrderCacheCoalesced.Inc()
		}
		if result.Err != nil {
			return domain.Order{}, false, time.Time{}, result.Err
		}
		row := result.Val.(loadResult)
		return row.order, row.archived, row.now, nil
	}
}

func (c *OrderCache) loadThrough(ctx context.Context, id uuid.UUID) (loadResult, error) {
	now := c.now().UTC()
	if entry, ok := c.l1.Get(id); ok && entry.valid(id, now) {
		return loadResult{entry.Order, true, now}, nil
	}
	if entry, ok := c.getRedis(ctx, id); ok {
		c.l1.Add(id, entry)
		return loadResult{entry.Order, true, c.now().UTC()}, nil
	}
	if !c.acquireDBSlot(ctx) {
		metrics.OrderReadShed.Inc()
		return loadResult{}, domain.ErrOverloaded
	}
	order, archived, dbNow, err := c.load(ctx, id)
	c.releaseDBSlot()
	if err != nil {
		return loadResult{}, err
	}

	// Archive rows are immutable. Live status and cost always come from PostgreSQL.
	if archived && order.DeleteAfter != nil {
		now = c.now().UTC()
		ttl := time.Duration(float64(c.cfg.TTL) * (0.8 + 0.4*rand.Float64()))
		expiry := minTime(now.Add(ttl), *order.DeleteAfter)
		entry := cachedEntry{Order: order, ExpiresAt: expiry}
		if entry.valid(id, now) {
			c.l1.Add(id, entry)
			c.setRedis(ctx, entry)
		}
	}
	return loadResult{order, archived, dbNow}, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func redisKey(id uuid.UUID) string { return "rental:order:archive:v1:" + id.String() }

// A generation prevents an old response from closing a circuit that opened
// while the request was in flight. After cooldown only one request probes Redis.
func (c *OrderCache) allowRedis(now time.Time) (uint64, bool) {
	c.redisMu.Lock()
	defer c.redisMu.Unlock()
	if !c.redisRetryAt.IsZero() {
		if now.Before(c.redisRetryAt) || c.redisProbing {
			return 0, false
		}
		c.redisProbing = true
	}
	return c.redisGeneration, true
}

func (c *OrderCache) recordRedisResult(generation uint64, err error) {
	c.redisMu.Lock()
	defer c.redisMu.Unlock()
	if generation != c.redisGeneration {
		return
	}
	probe := c.redisProbing
	c.redisProbing = false
	if err == nil {
		c.redisFailures = 0
		if probe {
			c.redisRetryAt = time.Time{}
			metrics.OrderCacheRedisCircuitOpen.Set(0)
		}
		return
	}
	c.redisFailures++
	if probe || c.redisFailures >= redisFailureThreshold {
		c.redisGeneration++
		c.redisRetryAt = c.now().Add(c.cfg.RedisCooldown)
		metrics.OrderCacheRedisCircuitOpen.Set(1)
	}
}

func (c *OrderCache) getRedis(ctx context.Context, id uuid.UUID) (cachedEntry, bool) {
	if c.remote == nil {
		return cachedEntry{}, false
	}
	generation, allowed := c.allowRedis(c.now())
	if !allowed {
		metrics.OrderCacheLookups.WithLabelValues("redis", "bypass").Inc()
		return cachedEntry{}, false
	}
	raw, found, err := c.remote.Get(ctx, redisKey(id))
	c.recordRedisResult(generation, err)
	if err != nil {
		metrics.OrderCacheLookups.WithLabelValues("redis", "error").Inc()
		return cachedEntry{}, false
	}
	var entry cachedEntry
	if !found || json.Unmarshal(raw, &entry) != nil || !entry.valid(id, c.now()) {
		metrics.OrderCacheLookups.WithLabelValues("redis", "miss").Inc()
		return cachedEntry{}, false
	}
	metrics.OrderCacheLookups.WithLabelValues("redis", "hit").Inc()
	return entry, true
}

func (c *OrderCache) setRedis(ctx context.Context, entry cachedEntry) {
	if c.remote == nil {
		return
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return
	}
	now := c.now()
	ttl := entry.ExpiresAt.Sub(now)
	if ttl <= 0 {
		return
	}
	generation, allowed := c.allowRedis(now)
	if allowed {
		c.recordRedisResult(generation, c.remote.SetPX(ctx, redisKey(entry.Order.ID), raw, ttl))
	}
}

func (c *OrderCache) dbConcurrencyLimit() int {
	c.redisMu.Lock()
	defer c.redisMu.Unlock()
	if !c.redisRetryAt.IsZero() {
		return c.cfg.DBDegradedConcurrency
	}
	return c.cfg.DBMaxConcurrency
}

func (c *OrderCache) acquireDBSlot(ctx context.Context) bool {
	c.dbMu.Lock()
	limit := c.dbConcurrencyLimit()
	if c.dbInflight < limit {
		c.dbInflight++
		metrics.OrderReadDBInflight.Inc()
		c.dbMu.Unlock()
		return true
	}
	if len(c.dbWaiters) >= dbWaitersPerSlot*limit {
		c.dbMu.Unlock()
		return false
	}
	ready := make(chan struct{})
	c.dbWaiters = append(c.dbWaiters, ready)
	c.dbMu.Unlock()

	timer := time.NewTimer(c.cfg.DBWaitTimeout)
	defer timer.Stop()
	select {
	case <-ready:
		return true
	case <-timer.C:
	case <-ctx.Done():
	}

	c.dbMu.Lock()
	for i, waiter := range c.dbWaiters {
		if waiter == ready {
			copy(c.dbWaiters[i:], c.dbWaiters[i+1:])
			c.dbWaiters[len(c.dbWaiters)-1] = nil
			c.dbWaiters = c.dbWaiters[:len(c.dbWaiters)-1]
			c.dbMu.Unlock()
			return false
		}
	}
	c.dbMu.Unlock()
	// A release assigned the slot at the same time as the timeout.
	c.releaseDBSlot()
	return false
}

func (c *OrderCache) releaseDBSlot() {
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	if len(c.dbWaiters) > 0 && c.dbInflight <= c.dbConcurrencyLimit() {
		next := c.dbWaiters[0]
		c.dbWaiters[0] = nil
		c.dbWaiters = c.dbWaiters[1:]
		close(next)
		return
	}
	c.dbInflight--
	metrics.OrderReadDBInflight.Dec()
}
