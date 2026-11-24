package offer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

const offerColumns = `offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee,
	deposit, user_profile_fallback_used, status, created_at, expires_at`

type offerRow struct {
	id                    uuid.UUID
	idempotencyID, userID string
	scooterID, zoneID     string
	charge                int
	minuteRate, unlockFee int64
	deposit               int64
	profileFallback       bool
	status                string
	createdAt, expiresAt  time.Time
}

func (r offerRow) model() (storedOffer, error) {
	request, err := newQuoteRequest(r.scooterID, r.userID, r.idempotencyID)
	if err != nil {
		return storedOffer{}, err
	}
	scooter := source.Scooter{ID: r.scooterID, ZoneID: r.zoneID, Charge: r.charge}
	terms := Terms{MinuteRate: r.minuteRate, UnlockFee: r.unlockFee, Deposit: r.deposit}
	q, err := newQuote(r.id, request, scooter, terms, r.profileFallback)
	if err != nil {
		return storedOffer{}, err
	}
	return restoreOffer(q, status(r.status), r.createdAt, r.expiresAt)
}

func scanOffer(row pgx.Row) (storedOffer, error) {
	var r offerRow
	if err := row.Scan(&r.id, &r.idempotencyID, &r.userID, &r.scooterID, &r.zoneID, &r.charge,
		&r.minuteRate, &r.unlockFee, &r.deposit, &r.profileFallback, &r.status, &r.createdAt, &r.expiresAt); err != nil {
		return storedOffer{}, err
	}
	o, err := r.model()
	if err != nil {
		// A corrupt row is an internal error, even if a constructor rejected client-like data.
		return storedOffer{}, fmt.Errorf("restore offer %s: %v", r.id, err)
	}
	return o, nil
}

type repository struct{ db postgres.DBTX }

func (r repository) findByKey(ctx context.Context, key string) (storedOffer, error) {
	return scanOffer(r.db.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE idempotency_id = $1`, key))
}

func (r repository) insert(ctx context.Context, q quote, ttl time.Duration) (storedOffer, bool, error) {
	row := r.db.QueryRow(ctx, `
		WITH offer_time AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate,
			unlock_fee, deposit, user_profile_fallback_used, status, created_at, expires_at)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'AVAILABLE', created_at,
			created_at + make_interval(secs => $11)
		FROM offer_time
		ON CONFLICT (idempotency_id) DO NOTHING
		RETURNING `+offerColumns,
		q.id, q.request.idempotencyID, q.request.userID, q.scooter.ID, q.scooter.ZoneID, q.scooter.Charge,
		q.terms.MinuteRate, q.terms.UnlockFee, q.terms.Deposit, q.profileFallback, ttl.Seconds())
	stored, err := scanOffer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedOffer{}, false, nil
	}
	return stored, err == nil, err
}

func (r repository) findByID(ctx context.Context, id uuid.UUID) (storedOffer, error) {
	return scanOffer(r.db.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE offer_id = $1`, id))
}

func (r repository) lockForStart(ctx context.Context, tx postgres.Tx, id uuid.UUID) (storedOffer, error) {
	return scanOffer(tx.QueryRow(ctx, `SELECT `+offerColumns+` FROM offers WHERE offer_id = $1 FOR UPDATE`, id))
}

func (r repository) consume(ctx context.Context, tx postgres.Tx, id uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE offers SET status = 'CONSUMED' WHERE offer_id = $1`, id)
	return err
}
