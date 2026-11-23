package source

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpjson"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return NewClient(server.URL, httpjson.NewClient(), 100*time.Millisecond, 100*time.Millisecond)
}

func TestClientClassification(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    error
	}{
		{"ok", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":"s-1","zone_id":"zone-1","charge":42}`))
		}, nil},
		{"not found", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }, ErrNotFound},
		{"5xx", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) }, ErrUnavailable},
		{"timeout", func(w http.ResponseWriter, r *http.Request) { time.Sleep(300 * time.Millisecond) }, ErrUnavailable},
		{"broken body", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{`)) }, ErrUnavailable},
		{"contract violation", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":"s-1","zone_id":"zone-1","charge":101}`))
		}, ErrUnavailable},
		{"missing charge", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id":"s-1","zone_id":"zone-1"}`))
		}, ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, tt.handler)
			scooter, err := client.Scooter(context.Background(), "s-1")
			if tt.want == nil {
				if err != nil || scooter != (domain.Scooter{ID: "s-1", ZoneID: "zone-1", Charge: 42}) {
					t.Fatalf("Scooter = %+v, %v", scooter, err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestClientPaths(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/tariff-zone/zone%2F1":
			_, _ = w.Write([]byte(`{"id":"zone/1","price_per_minute":10,"price_unlock":50,"default_deposit":300}`))
		case "/user-profile/u-1":
			_, _ = w.Write([]byte(`{"id":"u-1","has_subscription":true,"trusted":false}`))
		case "/configs":
			_, _ = w.Write([]byte(`{"price_coeff_settings":{"surge":1.25,"low_charge_discount":0.8},"default_retention_days":365}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	ctx := context.Background()
	if tariff, err := client.TariffZone(ctx, "zone/1"); err != nil || tariff.PriceUnlock != 50 {
		t.Fatalf("TariffZone = %+v, %v", tariff, err)
	}
	if profile, err := client.UserProfile(ctx, "u-1"); err != nil || !profile.HasSubscription || profile.Trusted {
		t.Fatalf("UserProfile = %+v, %v", profile, err)
	}
	if cfg, err := client.Configs(ctx); err != nil || cfg.PriceCoeffSettings.Surge.String() != "1.25" || cfg.TariffZoneCacheTTLMinutes != nil {
		t.Fatalf("Configs = %+v, %v", cfg, err)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestTariffCache(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	tariff := domain.Tariff{ZoneID: "zone-1", PricePerMinute: 10}
	var calls atomic.Int32
	var failWith error
	fetch := func(context.Context, string) (domain.Tariff, error) {
		calls.Add(1)
		if failWith != nil {
			return domain.Tariff{}, failWith
		}
		return tariff, nil
	}
	ttl := 10 * time.Minute
	cache, err := NewTariffCache(2, fetch, func() time.Duration { return ttl }, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := cache.Tariff(ctx, "zone-1"); err != nil || calls.Load() != 1 {
		t.Fatalf("miss: %v, calls=%d", err, calls.Load())
	}
	clock.now = clock.now.Add(9 * time.Minute)
	failWith = ErrUnavailable
	if _, err := cache.Tariff(ctx, "zone-1"); err != nil || calls.Load() != 1 {
		t.Fatalf("fresh hit must not call Source: %v, calls=%d", err, calls.Load())
	}

	clock.now = clock.now.Add(time.Minute)
	if _, err := cache.Tariff(ctx, "zone-1"); !errors.Is(err, domain.ErrZoneStale) {
		t.Fatalf("stale entry with Source down: %v", err)
	}
	if _, err := cache.Tariff(ctx, "zone-2"); !errors.Is(err, domain.ErrSourceUnavailable) {
		t.Fatalf("cold miss with Source down: %v", err)
	}

	failWith = nil
	if _, err := cache.Tariff(ctx, "zone-1"); err != nil {
		t.Fatalf("stale entry refreshed: %v", err)
	}

	ttl = time.Minute // a config change shortens the TTL of existing entries
	clock.now = clock.now.Add(2 * time.Minute)
	failWith = ErrUnavailable
	if _, err := cache.Tariff(ctx, "zone-1"); !errors.Is(err, domain.ErrZoneStale) {
		t.Fatalf("new TTL not applied: %v", err)
	}
}

// A cancelled caller that started the shared fetch must not fail the other callers.
func TestTariffCacheLeaderCancelled(t *testing.T) {
	tariff := domain.Tariff{ZoneID: "zone-1", PricePerMinute: 10}
	started := make(chan struct{})
	release := make(chan struct{})
	fetch := func(ctx context.Context, _ string) (domain.Tariff, error) {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return domain.Tariff{}, err
		}
		return tariff, nil
	}
	cache, err := NewTariffCache(2, fetch, func() time.Duration { return time.Minute }, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := cache.Tariff(leaderCtx, "zone-1")
		leaderErr <- err
	}()
	<-started

	followerResult := make(chan error, 1)
	go func() {
		got, err := cache.Tariff(context.Background(), "zone-1")
		if err == nil && got != tariff {
			err = errors.New("unexpected tariff")
		}
		followerResult <- err
	}()
	// Let the follower join the in-flight fetch before the leader goes away.
	time.Sleep(20 * time.Millisecond)

	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v", err)
	}
	close(release)
	if err := <-followerResult; err != nil {
		t.Fatalf("follower: %v", err)
	}
}
