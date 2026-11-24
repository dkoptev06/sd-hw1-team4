package offer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

type memoryOffers struct {
	offerRepository
	mu     sync.Mutex
	byKey  map[string]storedOffer
	before func() // runs before an insert, to simulate a concurrent request
}

func (m *memoryOffers) findByKey(_ context.Context, key string) (storedOffer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.byKey[key]
	if !ok {
		return storedOffer{}, pgx.ErrNoRows
	}
	return o, nil
}

func (m *memoryOffers) insert(_ context.Context, q quote, ttl time.Duration) (storedOffer, bool, error) {
	if m.before != nil {
		m.before()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byKey[q.request.idempotencyID]; ok {
		return storedOffer{}, false, nil
	}
	now := time.Now().UTC()
	o, err := restoreOffer(q, available, now, now.Add(ttl))
	if err != nil {
		return storedOffer{}, false, err
	}
	m.byKey[q.request.idempotencyID] = o
	return o, true, nil
}

type scootersFunc func(ctx context.Context, id string) (source.Scooter, error)

func (f scootersFunc) Scooter(ctx context.Context, id string) (source.Scooter, error) {
	return f(ctx, id)
}

type tariffsFunc func(ctx context.Context, zoneID string) (source.Tariff, error)

func (f tariffsFunc) Tariff(ctx context.Context, zoneID string) (source.Tariff, error) {
	return f(ctx, zoneID)
}

type profilesFunc func(ctx context.Context, id string) (source.UserProfile, error)

func (f profilesFunc) UserProfile(ctx context.Context, id string) (source.UserProfile, error) {
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
	service  *Service
	requests int
}

func newOfferFixture() *offerFixture {
	f := &offerFixture{store: &memoryOffers{byKey: map[string]storedOffer{}}}
	scooters := scootersFunc(func(_ context.Context, id string) (source.Scooter, error) {
		f.requests++
		return source.Scooter{ID: id, ZoneID: "zone-1", Charge: 20}, f.scooter
	})
	tariffs := tariffsFunc(func(_ context.Context, zoneID string) (source.Tariff, error) {
		return source.Tariff{ZoneID: zoneID, PricePerMinute: 10, PriceUnlock: 50, DefaultDeposit: 300}, f.tariff
	})
	profiles := profilesFunc(func(_ context.Context, id string) (source.UserProfile, error) {
		return source.UserProfile{UserID: id, Trusted: f.trusted}, f.profile
	})
	f.service = &Service{repo: f.store, scooters: scooters, tariffs: tariffs, profiles: profiles, configs: staticConfigs{dynconfig.Defaults()}, ttl: 2 * time.Minute}
	return f
}

func TestCreateOffer(t *testing.T) {
	ctx := context.Background()
	f := newOfferFixture()
	f.trusted = true

	offer, created, err := f.service.Create(ctx, "s-1", "u-1", "k1")
	if err != nil || !created {
		t.Fatalf("create: %v created=%t", err, created)
	}
	stored := f.store.byKey["k1"]
	// floor(10 × 1.25) = 12, low charge: floor(12 × 0.8) = 9; trusted: no deposit.
	if offer.Terms != (Terms{MinuteRate: 9, UnlockFee: 50, Deposit: 0}) || stored.profileFallback ||
		stored.scooter.ZoneID != "zone-1" || stored.scooter.Charge != 20 {
		t.Fatalf("unexpected offer %+v", offer)
	}

	f.trusted = false // a later profile change does not affect the stored offer
	replay, created, err := f.service.Create(ctx, "s-1", "u-1", "k1")
	if err != nil || created || replay.ID != offer.ID || replay.Terms != offer.Terms || f.requests != 1 {
		t.Fatalf("replay: %+v created=%t err=%v requests=%d", replay, created, err, f.requests)
	}

	if _, _, err := f.service.Create(ctx, "s-1", "u-2", "k1"); !errors.Is(err, ErrParameterConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestCreateOfferConcurrentInsert(t *testing.T) {
	f := newOfferFixture()
	request, err := newQuoteRequest("s-1", "u-1", "k1")
	if err != nil {
		t.Fatal(err)
	}
	q, err := newQuote(uuid.New(), request, source.Scooter{ID: "s-1", ZoneID: "zone-1", Charge: 20}, Terms{}, false)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	winner, err := restoreOffer(q, available, now, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	f.store.before = func() {
		f.store.mu.Lock()
		f.store.byKey["k1"] = winner
		f.store.mu.Unlock()
	}
	got, created, err := f.service.Create(context.Background(), "s-1", "u-1", "k1")
	if err != nil || created || got.ID != winner.id {
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
		{name: "unknown user", profile: notFound, want: ErrNotFound},
		{name: "unknown scooter", scooter: notFound, want: ErrNotFound},
		{name: "scooters unavailable", scooter: unavailable, want: ErrSourceUnavailable},
		{name: "stale zone", tariff: source.ErrZoneStale, want: source.ErrZoneStale},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newOfferFixture()
			f.trusted = true
			f.scooter, f.tariff, f.profile = tt.scooter, tt.tariff, tt.profile
			offer, _, err := f.service.Create(context.Background(), "s-1", "u-1", "k")
			if tt.want != nil {
				if !errors.Is(err, tt.want) {
					t.Fatalf("error = %v, want %v", err, tt.want)
				}
				if len(f.store.byKey) != 0 {
					t.Fatal("failed request stored an offer")
				}
				return
			}
			if err != nil || f.store.byKey["k"].profileFallback != tt.fallback || offer.Terms.Deposit != 300 || offer.Terms.UnlockFee != 50 {
				t.Fatalf("offer %+v err=%v", offer, err)
			}
		})
	}
}

func TestCreateOfferValidation(t *testing.T) {
	f := newOfferFixture()
	if _, _, err := f.service.Create(context.Background(), "s", "u", ""); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing idempotency_id: %v", err)
	}
}
