// Package rental implements the client operations of the Rental API.
package rental

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
)

// Scooters reads fresh scooter data from Source.
type Scooters interface {
	Scooter(ctx context.Context, id string) (domain.Scooter, error)
}

// Tariffs reads tariff zones through the cache and returns domain errors.
type Tariffs interface {
	Tariff(ctx context.Context, zoneID string) (domain.Tariff, error)
}

// Profiles reads user profiles. Only source.ErrUnavailable allows fallback pricing.
type Profiles interface {
	UserProfile(ctx context.Context, id string) (domain.UserProfile, error)
}

// Configs provides the current config snapshot.
type Configs interface {
	Current() dynconfig.Snapshot
}

// OfferStore persists offers. Lookups of a missing row return storage.ErrNotFound.
type OfferStore interface {
	FindOfferByIdempotencyID(ctx context.Context, key string) (domain.Offer, error)
	InsertOffer(ctx context.Context, offer domain.Offer, ttl time.Duration) (domain.Offer, bool, error)
}

// errNotStored means no offer uses the idempotency key yet.
var errNotStored = errors.New("offer not stored")

type OfferService struct {
	store    OfferStore
	scooters Scooters
	tariffs  Tariffs
	profiles Profiles
	configs  Configs
	ttl      time.Duration
}

func NewOfferService(store OfferStore, scooters Scooters, tariffs Tariffs, profiles Profiles, configs Configs, ttl time.Duration) *OfferService {
	return &OfferService{store: store, scooters: scooters, tariffs: tariffs, profiles: profiles, configs: configs, ttl: ttl}
}

// CreateOfferRequest is the input of POST /offers.
type CreateOfferRequest struct {
	ScooterID     string
	UserID        string
	IdempotencyID string
}

func (r CreateOfferRequest) validate() error {
	if strings.TrimSpace(r.ScooterID) == "" || strings.TrimSpace(r.UserID) == "" || strings.TrimSpace(r.IdempotencyID) == "" {
		return domain.Wrap(domain.ErrInvalidRequest, "scooter_id, user_id and idempotency_id are required")
	}
	return nil
}

// Create prices and stores an offer. A repeated request with the same idempotency
// key returns the stored offer with created=false.
func (s *OfferService) Create(ctx context.Context, req CreateOfferRequest) (offer domain.Offer, created bool, err error) {
	if err := req.validate(); err != nil {
		return domain.Offer{}, false, err
	}
	if existing, err := s.replay(ctx, req); !errors.Is(err, errNotStored) {
		return existing, false, err
	}

	scooter, tariff, profile, err := s.fetch(ctx, req)
	if err != nil {
		return domain.Offer{}, false, err
	}
	terms, err := domain.Price(tariff, profile, scooter, s.configs.Current().Coefficients)
	if err != nil {
		return domain.Offer{}, false, domain.Wrap(domain.ErrSourceUnavailable, "price for zone %q: %v", scooter.ZoneID, err)
	}
	offer = domain.Offer{
		ID:              uuid.New(),
		IdempotencyID:   req.IdempotencyID,
		UserID:          req.UserID,
		Scooter:         scooter,
		Terms:           terms,
		ProfileFallback: profile.Fallback,
	}
	stored, inserted, err := s.store.InsertOffer(ctx, offer, s.ttl)
	if err != nil {
		return domain.Offer{}, false, err
	}
	if !inserted {
		// A concurrent request with the same key won the insert.
		existing, err := s.replay(ctx, req)
		if errors.Is(err, errNotStored) {
			return domain.Offer{}, false, fmt.Errorf("offer with idempotency_id %q vanished", req.IdempotencyID)
		}
		return existing, false, err
	}
	if profile.Fallback {
		metrics.ProfileFallbacks.Inc()
	}
	return stored, true, nil
}

func (s *OfferService) replay(ctx context.Context, req CreateOfferRequest) (domain.Offer, error) {
	existing, err := s.store.FindOfferByIdempotencyID(ctx, req.IdempotencyID)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return domain.Offer{}, errNotStored
	case err != nil:
		return domain.Offer{}, err
	case !existing.MatchesRequest(req.UserID, req.ScooterID):
		return domain.Offer{}, domain.Wrap(domain.ErrParameterConflict, "idempotency_id %q was used with other parameters", req.IdempotencyID)
	}
	return existing, nil
}

// fetch reads the scooter and its tariff zone in parallel with the user profile.
func (s *OfferService) fetch(ctx context.Context, req CreateOfferRequest) (domain.Scooter, domain.Tariff, domain.UserProfile, error) {
	var (
		scooter domain.Scooter
		tariff  domain.Tariff
		profile domain.UserProfile
	)
	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		if scooter, err = s.scooters.Scooter(gctx, req.ScooterID); err != nil {
			return sourceError("scooter", req.ScooterID, err)
		}
		tariff, err = s.tariffs.Tariff(gctx, scooter.ZoneID)
		return err
	})
	group.Go(func() error {
		var err error
		profile, err = s.profiles.UserProfile(gctx, req.UserID)
		if errors.Is(err, source.ErrUnavailable) {
			profile, err = domain.FallbackProfile(req.UserID), nil
		}
		if err != nil {
			return sourceError("user", req.UserID, err)
		}
		return nil
	})
	err := group.Wait()
	return scooter, tariff, profile, err
}

// sourceError maps a Source client error to a client-visible error.
func sourceError(entity, id string, err error) error {
	if errors.Is(err, source.ErrNotFound) {
		return domain.Wrap(domain.ErrNotFound, "%s %q not found", entity, id)
	}
	return domain.Wrap(domain.ErrSourceUnavailable, "%s %q: %v", entity, id, err)
}
