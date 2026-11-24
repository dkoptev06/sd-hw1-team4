package offer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

type scooters interface {
	Scooter(context.Context, string) (source.Scooter, error)
}

type tariffs interface {
	Tariff(context.Context, string) (source.Tariff, error)
}

type profiles interface {
	UserProfile(context.Context, string) (source.UserProfile, error)
}

type configs interface {
	Current() dynconfig.Snapshot
}

type offerRepository interface {
	findByKey(context.Context, string) (storedOffer, error)
	insert(context.Context, quote, time.Duration) (storedOffer, bool, error)
	findByID(context.Context, uuid.UUID) (storedOffer, error)
	lockForStart(context.Context, postgres.Tx, uuid.UUID) (storedOffer, error)
	consume(context.Context, postgres.Tx, uuid.UUID) error
}

type Service struct {
	repo     offerRepository
	scooters scooters
	tariffs  tariffs
	profiles profiles
	configs  configs
	ttl      time.Duration
}

func NewService(db postgres.DBTX, scooters scooters, tariffs tariffs, profiles profiles, configs configs, ttl time.Duration) *Service {
	return &Service{repo: repository{db: db}, scooters: scooters, tariffs: tariffs, profiles: profiles, configs: configs, ttl: ttl}
}

// Receipt is the service result for a stored quote.
type Receipt struct {
	ID        uuid.UUID
	Terms     Terms
	ExpiresAt time.Time
}

func receipt(o storedOffer) Receipt {
	return Receipt{ID: o.id, Terms: o.terms, ExpiresAt: o.expiresAt}
}

// Create prices and stores an offer or returns the result of an earlier request.
func (s *Service) Create(ctx context.Context, scooterID, userID, idempotencyID string) (Receipt, bool, error) {
	request, err := newQuoteRequest(scooterID, userID, idempotencyID)
	if err != nil {
		return Receipt{}, false, err
	}
	if existing, err := s.replay(ctx, request); !errors.Is(err, pgx.ErrNoRows) {
		return existing, false, err
	}

	scooter, tariff, profile, err := s.fetch(ctx, request)
	if err != nil {
		return Receipt{}, false, err
	}
	q, err := priceQuote(uuid.New(), request, scooter, tariff, profile, s.configs.Current().Coefficients)
	if err != nil {
		return Receipt{}, false, fmt.Errorf("%w: price for zone %q: %v", ErrSourceUnavailable, scooter.ZoneID, err)
	}
	stored, inserted, err := s.repo.insert(ctx, q, s.ttl)
	if err != nil {
		return Receipt{}, false, err
	}
	if !inserted {
		existing, err := s.replay(ctx, request)
		if errors.Is(err, pgx.ErrNoRows) {
			return Receipt{}, false, fmt.Errorf("offer with idempotency_id %q vanished", request.idempotencyID)
		}
		return existing, false, err
	}
	if q.profileFallback {
		metrics.ProfileFallbacks.Inc()
	}
	return receipt(stored), true, nil
}

func (s *Service) replay(ctx context.Context, request quoteRequest) (Receipt, error) {
	existing, err := s.repo.findByKey(ctx, request.idempotencyID)
	if err != nil {
		return Receipt{}, err
	}
	if err := existing.checkReplay(request); err != nil {
		return Receipt{}, err
	}
	return receipt(existing), nil
}

// FindScooterForStart reads the quote without holding a lock across the Source call.
func (s *Service) FindScooterForStart(ctx context.Context, id uuid.UUID) (string, error) {
	o, err := s.repo.findByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: offer %s not found", ErrNotFound, id)
	}
	if err != nil {
		return "", err
	}
	if err := o.checkAvailable(); err != nil {
		return "", err
	}
	return o.scooter.ID, nil
}

// LockedOffer is the offer held by an order-creation transaction.
type LockedOffer struct{ value storedOffer }

func (o LockedOffer) ScooterID() string { return o.value.scooter.ID }

func (s *Service) LockForStart(ctx context.Context, tx postgres.Tx, id uuid.UUID) (LockedOffer, error) {
	o, err := s.repo.lockForStart(ctx, tx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return LockedOffer{}, fmt.Errorf("%w: offer %s not found", ErrNotFound, id)
	}
	return LockedOffer{value: o}, err
}

// StartTerms are the immutable values copied to a new order.
type StartTerms struct {
	UserID  string
	Scooter source.Scooter
	Terms   Terms
}

func (s *Service) CheckStart(locked LockedOffer, now time.Time, current source.Scooter) (StartTerms, error) {
	o := locked.value
	if err := o.checkStart(now, current); err != nil {
		return StartTerms{}, err
	}
	return StartTerms{UserID: o.request.userID, Scooter: o.scooter, Terms: o.terms}, nil
}

func (s *Service) Consume(ctx context.Context, tx postgres.Tx, locked LockedOffer) error {
	return s.repo.consume(ctx, tx, locked.value.id)
}

// fetch reads the scooter and its tariff zone in parallel with the user profile.
func (s *Service) fetch(ctx context.Context, request quoteRequest) (source.Scooter, source.Tariff, pricingProfile, error) {
	var (
		scooter source.Scooter
		tariff  source.Tariff
		profile pricingProfile
	)
	group, gctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		var err error
		if scooter, err = s.scooters.Scooter(gctx, request.scooterID); err != nil {
			return sourceError("scooter", request.scooterID, err)
		}
		tariff, err = s.tariffs.Tariff(gctx, scooter.ZoneID)
		return err
	})
	group.Go(func() error {
		var err error
		profile.UserProfile, err = s.profiles.UserProfile(gctx, request.userID)
		if errors.Is(err, source.ErrUnavailable) {
			profile, err = fallbackProfile(request.userID), nil
		}
		if err != nil {
			return sourceError("user", request.userID, err)
		}
		return nil
	})
	err := group.Wait()
	return scooter, tariff, profile, err
}

func sourceError(entity, id string, err error) error {
	if errors.Is(err, source.ErrNotFound) {
		return fmt.Errorf("%w: %s %q not found", ErrNotFound, entity, id)
	}
	return fmt.Errorf("%w: %s %q: %v", ErrSourceUnavailable, entity, id, err)
}
