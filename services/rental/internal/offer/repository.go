package offer

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

func lockIdempotencyKey(ctx context.Context, tx postgres.Tx, key string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key)
	return err
}

func lookup(ctx context.Context, db postgres.DBTX, key, userID, scooterID string) (Receipt, error) {
	var receipt Receipt
	var storedUser, storedScooter string
	err := db.QueryRow(ctx, `
		SELECT offer_id, minute_rate, unlock_fee, deposit, expires_at, user_id, scooter_id
		FROM offers WHERE idempotency_id = $1`, key).Scan(
		&receipt.OfferID, &receipt.MinuteRate, &receipt.UnlockFee, &receipt.Deposit, &receipt.ExpiresAt,
		&storedUser, &storedScooter)
	if errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, ErrNotFound
	}
	if err != nil {
		return Receipt{}, err
	}
	if userID != storedUser || scooterID != storedScooter {
		return Receipt{}, ErrParameterConflict
	}
	receipt.ExpiresAt = receipt.ExpiresAt.UTC()
	receipt.Replayed = true
	return receipt, nil
}

func insert(ctx context.Context, tx postgres.Tx, offerID uuid.UUID, quote Quote) (Receipt, error) {
	receipt := Receipt{
		OfferID: offerID, MinuteRate: quote.minuteRate, UnlockFee: quote.unlockFee, Deposit: quote.deposit,
	}
	err := tx.QueryRow(ctx, `
		INSERT INTO offers (
			offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee,
			deposit, user_profile_fallback_used, status, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'AVAILABLE',
			clock_timestamp() + $11 * interval '1 second')
		RETURNING expires_at`, receipt.OfferID, quote.idempotencyID, quote.userID, quote.scooter.id, quote.scooter.zoneID,
		quote.scooter.charge, quote.minuteRate, quote.unlockFee, quote.deposit, quote.profileFallbackUsed, ttl.Seconds()).
		Scan(&receipt.ExpiresAt)
	receipt.ExpiresAt = receipt.ExpiresAt.UTC()
	return receipt, err
}

func findScooterForStart(ctx context.Context, db postgres.DBTX, offerID uuid.UUID) (scooterID, status string, expiresAt, now time.Time, err error) {
	err = db.QueryRow(ctx, `
		SELECT scooter_id, status, expires_at, clock_timestamp()
		FROM offers WHERE offer_id = $1`, offerID).
		Scan(&scooterID, &status, &expiresAt, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return
}

func lockForStart(ctx context.Context, tx postgres.Tx, offerID uuid.UUID) (StartSnapshot, error) {
	var scooterID, zoneID, userID, status string
	var charge int
	var minuteRate, unlockFee, deposit int64
	var expiresAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee, deposit, status, expires_at
		FROM offers WHERE offer_id = $1 FOR UPDATE`, offerID).
		Scan(&userID, &scooterID, &zoneID, &charge, &minuteRate, &unlockFee, &deposit, &status, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return StartSnapshot{}, ErrNotFound
	}
	if err != nil {
		return StartSnapshot{}, err
	}
	scooter, err := NewScooterSnapshot(scooterID, zoneID, charge)
	if err != nil {
		return StartSnapshot{}, err
	}
	return newStartSnapshot(scooter, userID, minuteRate, unlockFee, deposit, status, expiresAt)
}

func consume(ctx context.Context, tx postgres.Tx, offerID uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE offers SET status = 'CONSUMED' WHERE offer_id = $1`, offerID)
	return err
}
