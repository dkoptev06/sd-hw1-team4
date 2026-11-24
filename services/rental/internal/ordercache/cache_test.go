package ordercache

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

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
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

func testCache(t *testing.T, remote RemoteCache, loader DBLoader, now func() time.Time) *OrderCache {
	t.Helper()
	cfg := settings.OrderCache{
		Size: 16, TTL: time.Minute, RedisCooldown: time.Second,
		DBMaxConcurrency: 4, DBDegradedConcurrency: 1, DBWaitTimeout: 25 * time.Millisecond,
	}
	cache, err := New(cfg, remote, loader, now)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestArchivedCacheHonorsExpiryAndPreservesLargeCost(t *testing.T) {
	remote := &memoryRedis{items: make(map[string][]byte)}
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	expiry := now.Add(20 * time.Second)
	cost := new(big.Int).Lsh(big.NewInt(1), 70)
	id := uuid.New()
	var dbReads int
	load := func(_ context.Context, _ uuid.UUID) (domain.Order, bool, time.Time, error) {
		dbReads++
		if !now.Before(expiry) {
			return domain.Order{}, false, now, storage.ErrNotFound
		}
		return domain.Order{ID: id, Status: domain.OrderCompleted, DeleteAfter: &expiry, Cost: cost}, true, now, nil
	}
	first := testCache(t, remote, load, clock)
	second := testCache(t, remote, load, clock)
	for _, cache := range []*OrderCache{first, first, second} {
		row, archived, _, err := cache.Get(t.Context(), id)
		if err != nil || !archived || row.Cost == nil || row.Cost.Cmp(cost) != 0 {
			t.Fatalf("cached row: %+v archived=%t err=%v", row, archived, err)
		}
	}
	if dbReads != 1 || remote.calls.Load() != 2 {
		t.Fatalf("expected L1 and L2 hits, DB=%d Redis=%d", dbReads, remote.calls.Load())
	}
	now = expiry
	if _, _, _, err := second.Get(t.Context(), id); !errors.Is(err, storage.ErrNotFound) || dbReads != 2 {
		t.Fatalf("expired cache was used: DB=%d err=%v", dbReads, err)
	}
}

func TestCurrentOrderStatusAndCostStayFresh(t *testing.T) {
	remote := &memoryRedis{items: make(map[string][]byte)}
	id := uuid.New()
	currentStatus := domain.OrderStarting
	load := func(_ context.Context, _ uuid.UUID) (domain.Order, bool, time.Time, error) {
		return domain.Order{ID: id, Status: currentStatus, Cost: big.NewInt(42)}, false, time.Now(), nil
	}
	cache := testCache(t, remote, load, time.Now)
	for _, status := range []domain.OrderStatus{domain.OrderStarting, domain.OrderActive, domain.OrderCompleted} {
		currentStatus = status
		row, archived, _, err := cache.Get(t.Context(), id)
		if err != nil || archived || row.Status != status {
			t.Fatalf("expected current %s, got %s archived=%t err=%v", status, row.Status, archived, err)
		}
	}
}

func TestConcurrentMissesShareOneDBRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		id := uuid.New()
		release := make(chan struct{})
		var calls atomic.Int32
		load := func(_ context.Context, _ uuid.UUID) (domain.Order, bool, time.Time, error) {
			calls.Add(1)
			<-release
			return domain.Order{ID: id, Status: domain.OrderActive}, false, time.Now(), nil
		}
		cache := testCache(t, nil, load, time.Now)
		var readers sync.WaitGroup
		for range 32 {
			readers.Go(func() {
				order, archived, _, err := cache.Get(t.Context(), id)
				if err != nil || archived || order.ID != id {
					t.Errorf("shared result: order=%v archived=%t err=%v", order.ID, archived, err)
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
	load := func(ctx context.Context, id uuid.UUID) (domain.Order, bool, time.Time, error) {
		now := clock()
		if id == warm {
			expiry := now.Add(time.Hour)
			return domain.Order{ID: id, Status: domain.OrderCompleted, DeleteAfter: &expiry}, true, now, nil
		}
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		firstCold.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return domain.Order{}, false, now, ctx.Err()
		case <-release:
			return domain.Order{}, false, now, storage.ErrNotFound
		}
	}
	cache := testCache(t, remote, load, clock)
	if _, _, _, err := cache.Get(t.Context(), warm); err != nil {
		t.Fatal(err)
	}
	remote.down.Store(true)
	for range redisFailureThreshold - 1 {
		cache.getRedis(t.Context(), uuid.New())
	}
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	go cache.Get(caller, uuid.New())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fallback failed to start")
	}
	if _, _, _, err := cache.Get(t.Context(), warm); err != nil {
		t.Fatalf("warm read during outage: %v", err)
	}

	var readers sync.WaitGroup
	for range 32 {
		readers.Go(func() {
			if _, _, _, err := cache.Get(t.Context(), uuid.New()); !errors.Is(err, domain.ErrOverloaded) {
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
	if _, _, _, err := cache.Get(t.Context(), warm); err != nil {
		t.Fatalf("Redis probe: %v", err)
	}
	if cache.dbConcurrencyLimit() != 4 || remote.calls.Load() != 2+redisFailureThreshold {
		t.Fatalf("circuit did not recover: DB limit=%d Redis calls=%d", cache.dbConcurrencyLimit(), remote.calls.Load())
	}
}
