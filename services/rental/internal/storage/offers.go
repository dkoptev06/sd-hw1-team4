package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
)

const offerColumns = `offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee,
	deposit, user_profile_fallback_used, status, created_at, expires_at`

func scanOffer(row pgx.Row) (domain.Offer, error) {
	var o domain.Offer
	err := row.Scan(&o.ID, &o.IdempotencyID, &o.UserID, &o.Scooter.ID, &o.Scooter.ZoneID, &o.Scooter.Charge,
		&o.Terms.MinuteRate, &o.Terms.UnlockFee, &o.Terms.Deposit, &o.ProfileFallback, &o.Status,
		&o.CreatedAt, &o.ExpiresAt)
	if err != nil {
		return domain.Offer{}, notFound(err)
	}
	o.CreatedAt, o.ExpiresAt = o.CreatedAt.UTC(), o.ExpiresAt.UTC()
	return o, nil
}

// FindOfferByIdempotencyID returns the offer created by an earlier request with the same key.
func (q *Queries) FindOfferByIdempotencyID(ctx context.Context, key string) (domain.Offer, error) {
	return scanOffer(q.db.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE idempotency_id = $1`, key))
}

// InsertOffer stores a new AVAILABLE offer valid for ttl from the database clock.
// It returns inserted=false if another request already used the idempotency key.
func (q *Queries) InsertOffer(ctx context.Context, o domain.Offer, ttl time.Duration) (domain.Offer, bool, error) {
	row := q.db.QueryRow(ctx, `
		WITH offer_time AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate,
			unlock_fee, deposit, user_profile_fallback_used, status, created_at, expires_at)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'AVAILABLE', created_at,
			created_at + make_interval(secs => $11)
		FROM offer_time
		ON CONFLICT (idempotency_id) DO NOTHING
		RETURNING `+offerColumns,
		o.ID, o.IdempotencyID, o.UserID, o.Scooter.ID, o.Scooter.ZoneID, o.Scooter.Charge,
		o.Terms.MinuteRate, o.Terms.UnlockFee, o.Terms.Deposit, o.ProfileFallback, ttl.Seconds())
	stored, err := scanOffer(row)
	if errors.Is(err, ErrNotFound) {
		return domain.Offer{}, false, nil
	}
	return stored, err == nil, err
}

// GetOffer reads an offer without locking it.
func (q *Queries) GetOffer(ctx context.Context, id uuid.UUID) (domain.Offer, error) {
	return scanOffer(q.db.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE offer_id = $1`, id))
}

// LockOffer reads and locks an offer for the start transaction.
func (q *Queries) LockOffer(ctx context.Context, id uuid.UUID) (domain.Offer, error) {
	return scanOffer(q.db.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE offer_id = $1 FOR UPDATE`, id))
}

func (q *Queries) ConsumeOffer(ctx context.Context, id uuid.UUID) error {
	_, err := q.db.Exec(ctx, `UPDATE offers SET status = 'CONSUMED' WHERE offer_id = $1`, id)
	return err
}
