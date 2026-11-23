package rental

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

type memoryOffers struct {
	mu     sync.Mutex
	byKey  map[string]domain.Offer
	before func() // runs before an insert, to simulate a concurrent request
}

func (m *memoryOffers) FindOfferByIdempotencyID(_ context.Context, key string) (domain.Offer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.byKey[key]
	if !ok {
		return domain.Offer{}, storage.ErrNotFound
	}
	return o, nil
}

func (m *memoryOffers) InsertOffer(_ context.Context, o domain.Offer, ttl time.Duration) (domain.Offer, bool, error) {
	if m.before != nil {
		m.before()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byKey[o.IdempotencyID]; ok {
		return domain.Offer{}, false, nil
	}
	o.Status, o.CreatedAt = domain.OfferAvailable, time.Now().UTC()
	o.ExpiresAt = o.CreatedAt.Add(ttl)
	m.byKey[o.IdempotencyID] = o
	return o, true, nil
}

type scootersFunc func(ctx context.Context, id string) (domain.Scooter, error)

func (f scootersFunc) Scooter(ctx context.Context, id string) (domain.Scooter, error) {
	return f(ctx, id)
}

type tariffsFunc func(ctx context.Context, zoneID string) (domain.Tariff, error)

func (f tariffsFunc) Tariff(ctx context.Context, zoneID string) (domain.Tariff, error) {
	return f(ctx, zoneID)
}

type profilesFunc func(ctx context.Context, id string) (domain.UserProfile, error)

func (f profilesFunc) UserProfile(ctx context.Context, id string) (domain.UserProfile, error) {
	return f(ctx, id)
}

type staticConfigs struct{ snapshot dynconfig.Snapshot }

func (c staticConfigs) Current() dynconfig.Snapshot { return c.snapshot }

type offerFixture struct {
	store    *memoryOffers
	scooter  error
	tariff   error
	profile  error
	trusted  bool
	service  *OfferService
	requests int
}

func newOfferFixture() *offerFixture {
	f := &offerFixture{store: &memoryOffers{byKey: map[string]domain.Offer{}}}
	scooters := scootersFunc(func(_ context.Context, id string) (domain.Scooter, error) {
		f.requests++
		return domain.Scooter{ID: id, ZoneID: "zone-1", Charge: 20}, f.scooter
	})
	tariffs := tariffsFunc(func(_ context.Context, zoneID string) (domain.Tariff, error) {
		return domain.Tariff{ZoneID: zoneID, PricePerMinute: 10, PriceUnlock: 50, DefaultDeposit: 300}, f.tariff
	})
	profiles := profilesFunc(func(_ context.Context, id string) (domain.UserProfile, error) {
		return domain.UserProfile{UserID: id, Trusted: f.trusted}, f.profile
	})
	f.service = NewOfferService(f.store, scooters, tariffs, profiles, staticConfigs{dynconfig.Defaults()}, 2*time.Minute)
	return f
}

func request(key string) CreateOfferRequest {
	return CreateOfferRequest{ScooterID: "s-1", UserID: "u-1", IdempotencyID: key}
}

func TestCreateOffer(t *testing.T) {
	ctx := context.Background()
	f := newOfferFixture()
	f.trusted = true

	offer, created, err := f.service.Create(ctx, request("k1"))
	if err != nil || !created {
		t.Fatalf("create: %v created=%t", err, created)
	}
	// floor(10 × 1.25) = 12, low charge: floor(12 × 0.8) = 9; trusted: no deposit.
	if offer.Terms != (domain.Terms{MinuteRate: 9, UnlockFee: 50, Deposit: 0}) || offer.ProfileFallback ||
		offer.Scooter.ZoneID != "zone-1" || offer.Scooter.Charge != 20 {
		t.Fatalf("unexpected offer %+v", offer)
	}

	f.trusted = false // a later profile change does not affect the stored offer
	replay, created, err := f.service.Create(ctx, request("k1"))
	if err != nil || created || replay.ID != offer.ID || replay.Terms != offer.Terms || f.requests != 1 {
		t.Fatalf("replay: %+v created=%t err=%v requests=%d", replay, created, err, f.requests)
	}

	other := request("k1")
	other.UserID = "u-2"
	if _, _, err := f.service.Create(ctx, other); !errors.Is(err, domain.ErrParameterConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestCreateOfferConcurrentInsert(t *testing.T) {
	f := newOfferFixture()
	winner := domain.Offer{IdempotencyID: "k1", UserID: "u-1", Scooter: domain.Scooter{ID: "s-1"}}
	f.store.before = func() {
		f.store.mu.Lock()
		f.store.byKey["k1"] = winner
		f.store.mu.Unlock()
	}
	got, created, err := f.service.Create(context.Background(), request("k1"))
	if err != nil || created || got.IdempotencyID != "k1" {
		t.Fatalf("lost insert race: %+v created=%t err=%v", got, created, err)
	}
}

func TestCreateOfferSourceFailures(t *testing.T) {
	unavailable := fmt.Errorf("%w: timeout", source.ErrUnavailable)
	notFound := fmt.Errorf("%w", source.ErrNotFound)
	tests := []struct {
		name                     string
		scooter, tariff, profile error
		want                     error
		fallback                 bool
	}{
		{name: "users unavailable gives greedy pricing", profile: unavailable, fallback: true},
		{name: "unknown user", profile: notFound, want: domain.ErrNotFound},
		{name: "unknown scooter", scooter: notFound, want: domain.ErrNotFound},
		{name: "scooters unavailable", scooter: unavailable, want: domain.ErrSourceUnavailable},
		{name: "stale zone", tariff: domain.ErrZoneStale, want: domain.ErrZoneStale},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newOfferFixture()
			f.trusted = true
			f.scooter, f.tariff, f.profile = tt.scooter, tt.tariff, tt.profile
			offer, _, err := f.service.Create(context.Background(), request("k"))
			if tt.want != nil {
				if !errors.Is(err, tt.want) {
					t.Fatalf("error = %v, want %v", err, tt.want)
				}
				if len(f.store.byKey) != 0 {
					t.Fatal("failed request stored an offer")
				}
				return
			}
			if err != nil || offer.ProfileFallback != tt.fallback || offer.Terms.Deposit != 300 || offer.Terms.UnlockFee != 50 {
				t.Fatalf("offer %+v err=%v", offer, err)
			}
		})
	}
}

func TestCreateOfferValidation(t *testing.T) {
	f := newOfferFixture()
	if _, _, err := f.service.Create(context.Background(), CreateOfferRequest{ScooterID: "s", UserID: "u"}); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("missing idempotency_id: %v", err)
	}
}
