package order

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
)

type memoryRedis struct {
	mu    sync.Mutex
	items map[string][]byte
	calls atomic.Int32
	down  atomic.Bool
}

func (r *memoryRedis) Get(_ context.Context, key string) ([]byte, bool, error) {
	r.calls.Add(1)
	if r.down.Load() {
		return nil, false, errors.New("redis unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value, found := r.items[key]
	return value, found, nil
}

func (r *memoryRedis) SetPX(_ context.Context, key string, val []byte, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[key] = val
	return nil
}

func (*memoryRedis) Close() error { return nil }

func testCache(t *testing.T, remote RemoteCache, loader orderLoader, now func() time.Time) *archiveCache {
	t.Helper()
	cfg := CacheOptions{
		Size: 16, TTL: time.Minute, RedisCooldown: time.Second,
		DBMaxConcurrency: 4, DBDegradedConcurrency: 1, DBWaitTimeout: 25 * time.Millisecond,
	}
	cache, err := newArchiveCache(cfg, remote, loader, now)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func cacheTrip(t *testing.T, id uuid.UUID, status Status, now time.Time, deleteAfter *time.Time, cost *big.Int) trip {
	t.Helper()
	row, err := restoreTrip(id, uuid.New(), "user", "scooter", "zone", offer.Terms{}, state{
		status: status, createdAt: now, deleteAfter: deleteAfter, cost: cost,
	})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestArchivedCacheHonorsExpiryAndPreservesLargeCost(t *testing.T) {
	remote := &memoryRedis{items: make(map[string][]byte)}
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	expiry := now.Add(20 * time.Second)
	cost := new(big.Int).Lsh(big.NewInt(1), 70)
	id := uuid.New()
	var dbReads int
	load := func(_ context.Context, _ uuid.UUID) (trip, bool, time.Time, error) {
		dbReads++
		if !now.Before(expiry) {
			return trip{}, false, now, pgx.ErrNoRows
		}
		return cacheTrip(t, id, Completed, now, &expiry, cost), true, now, nil
	}
	first := testCache(t, remote, load, clock)
	second := testCache(t, remote, load, clock)
	for _, cache := range []*archiveCache{first, first, second} {
		row, archived, _, err := cache.get(t.Context(), id)
		if err != nil || !archived || row.cost == nil || row.cost.Cmp(cost) != 0 {
			t.Fatalf("cached row: %+v archived=%t err=%v", row, archived, err)
		}
	}
	if dbReads != 1 || remote.calls.Load() != 2 {
		t.Fatalf("expected L1 and L2 hits, DB=%d Redis=%d", dbReads, remote.calls.Load())
	}
	now = expiry
	if _, _, _, err := second.get(t.Context(), id); !errors.Is(err, pgx.ErrNoRows) || dbReads != 2 {
		t.Fatalf("expired cache was used: DB=%d err=%v", dbReads, err)
	}
}

func TestCurrentOrderStatusAndCostStayFresh(t *testing.T) {
	remote := &memoryRedis{items: make(map[string][]byte)}
	id := uuid.New()
	currentStatus := Starting
	load := func(_ context.Context, _ uuid.UUID) (trip, bool, time.Time, error) {
		return cacheTrip(t, id, currentStatus, time.Now(), nil, big.NewInt(42)), false, time.Now(), nil
	}
	cache := testCache(t, remote, load, time.Now)
	for _, status := range []Status{Starting, Active, Completed} {
		currentStatus = status
		row, archived, _, err := cache.get(t.Context(), id)
		if err != nil || archived || row.status != status {
			t.Fatalf("expected current %s, got %s archived=%t err=%v", status, row.status, archived, err)
		}
	}
}

func TestConcurrentMissesShareOneDBRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := uuid.New()
		release := make(chan struct{})
		var calls atomic.Int32
		load := func(_ context.Context, _ uuid.UUID) (trip, bool, time.Time, error) {
			calls.Add(1)
			<-release
			return cacheTrip(t, id, Active, time.Now(), nil, nil), false, time.Now(), nil
		}
		cache := testCache(t, nil, load, time.Now)
		var readers sync.WaitGroup
		for range 32 {
			readers.Go(func() {
				order, archived, _, err := cache.get(t.Context(), id)
				if err != nil || archived || order.id != id {
					t.Errorf("shared result: order=%v archived=%t err=%v", order.id, archived, err)
				}
			})
		}
		synctest.Wait()
		close(release)
		readers.Wait()
		if n := calls.Load(); n != 1 {
			t.Fatalf("expected one coalesced read, got %d", n)
		}
	})
}

func TestRedisOutageBoundsDBFallbackAndRecovers(t *testing.T) {
	remote := &memoryRedis{items: make(map[string][]byte)}
	start := time.Now()
	var offset atomic.Int64
	clock := func() time.Time { return start.Add(time.Duration(offset.Load())) }
	warm := uuid.New()
	inflight := atomic.Int32{}
	peak := atomic.Int32{}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var firstCold sync.Once
	load := func(ctx context.Context, id uuid.UUID) (trip, bool, time.Time, error) {
		now := clock()
		if id == warm {
			expiry := now.Add(time.Hour)
			return cacheTrip(t, id, Completed, now, &expiry, nil), true, now, nil
		}
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		firstCold.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return trip{}, false, now, ctx.Err()
		case <-release:
			return trip{}, false, now, pgx.ErrNoRows
		}
	}
	cache := testCache(t, remote, load, clock)
	if _, _, _, err := cache.get(t.Context(), warm); err != nil {
		t.Fatal(err)
	}
	remote.down.Store(true)
	for range redisFailureThreshold - 1 {
		cache.getRedis(t.Context(), uuid.New())
	}
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	go cache.get(caller, uuid.New())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fallback failed to start")
	}
	if _, _, _, err := cache.get(t.Context(), warm); err != nil {
		t.Fatalf("warm read during outage: %v", err)
	}

	var readers sync.WaitGroup
	for range 32 {
		readers.Go(func() {
			if _, _, _, err := cache.get(t.Context(), uuid.New()); !errors.Is(err, ErrOverloaded) {
				t.Errorf("expected overload response, got %v", err)
			}
		})
	}
	readers.Wait()
	if peak.Load() != 1 || remote.calls.Load() != 1+redisFailureThreshold {
		t.Fatalf("outage caused extra calls: DB peak=%d Redis calls=%d", peak.Load(), remote.calls.Load())
	}
	remote.down.Store(false)
	offset.Store(int64(2 * time.Second))
	cache.l1.Remove(warm)
	if _, _, _, err := cache.get(t.Context(), warm); err != nil {
		t.Fatalf("Redis probe: %v", err)
	}
	if cache.dbConcurrencyLimit() != 4 || remote.calls.Load() != 2+redisFailureThreshold {
		t.Fatalf("circuit did not recover: DB limit=%d Redis calls=%d", cache.dbConcurrencyLimit(), remote.calls.Load())
	}
}
