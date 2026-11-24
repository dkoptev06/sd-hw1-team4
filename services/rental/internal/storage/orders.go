package storage

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
)

// Both orders and orders_history use the same order columns.
const orderColumnsBeforeCost = `order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
	status, created_at, started_at, finish_time, closed_at, delete_after`
const orderColumns = orderColumnsBeforeCost + `, cost`
const orderReadColumns = orderColumnsBeforeCost + `, cost::text`

func normalizeOrder(o *domain.Order) {
	o.CreatedAt = o.CreatedAt.UTC()
	o.StartedAt, o.FinishTime = utc(o.StartedAt), utc(o.FinishTime)
	o.ClosedAt, o.DeleteAfter = utc(o.ClosedAt), utc(o.DeleteAfter)
}

func scanOrder(row pgx.Row, extra ...any) (domain.Order, error) {
	var o domain.Order
	var cost *string
	dest := []any{&o.ID, &o.OfferID, &o.UserID, &o.ScooterID, &o.ZoneID, &o.Terms.MinuteRate,
		&o.Terms.UnlockFee, &o.Terms.Deposit, &o.Status, &o.CreatedAt, &o.StartedAt, &o.FinishTime,
		&o.ClosedAt, &o.DeleteAfter, &cost}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return domain.Order{}, notFound(err)
	}
	if cost != nil {
		amount, ok := new(big.Int).SetString(*cost, 10)
		if !ok {
			return domain.Order{}, fmt.Errorf("invalid stored cost for order %s: %q", o.ID, *cost)
		}
		o.Cost = amount
	}
	normalizeOrder(&o)
	return o, nil
}

// OrderRef identifies an order and its current status.
type OrderRef struct {
	ID     uuid.UUID
	Status domain.OrderStatus
}

// FindOrderByOffer reads the current order or its archived record in one snapshot.
func (q *Queries) FindOrderByOffer(ctx context.Context, offerID uuid.UUID) (OrderRef, error) {
	// Archiving removes the offer together with its order. Skip history for live offers.
	var ref OrderRef
	err := q.db.QueryRow(ctx, `
		SELECT order_id, status FROM orders WHERE offer_id = $1
		UNION ALL
		SELECT order_id, status FROM orders_history
		WHERE offer_id = $1 AND NOT EXISTS (SELECT 1 FROM offers WHERE offer_id = $1)
		LIMIT 1`, offerID).Scan(&ref.ID, &ref.Status)
	return ref, notFound(err)
}

// LockScooterForStart serializes starts for a scooter even when it has no order yet.
// The lock is held until the start transaction ends.
func (q *Queries) LockScooterForStart(ctx context.Context, scooterID string) error {
	_, err := q.db.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 1))", scooterID)
	return err
}

// LockOccupyingOrder locks the STARTING or ACTIVE order of a scooter, if any, with the
// deadline of its START task.
func (q *Queries) LockOccupyingOrder(ctx context.Context, scooterID string) (domain.Order, *time.Time, error) {
	var deadline *time.Time
	order, err := scanOrder(q.db.QueryRow(ctx, `
		SELECT o.order_id, o.offer_id, o.user_id, o.scooter_id, o.zone_id, o.minute_rate, o.unlock_fee, o.deposit,
			o.status, o.created_at, o.started_at, o.finish_time, o.closed_at, o.delete_after, o.cost::text, t.deadline_at
		FROM orders o
		LEFT JOIN tasks t ON t.order_id = o.order_id AND t.task_type = 'START'
		WHERE o.scooter_id = $1 AND o.status IN ('STARTING', 'ACTIVE')
		FOR UPDATE OF o`, scooterID), &deadline)
	return order, utc(deadline), err
}

// InsertOrder stores a new order. A concurrent start on the same scooter violates
// the one-active-order index and is reported as SCOOTER_BUSY.
func (q *Queries) InsertOrder(ctx context.Context, o domain.Order) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
			status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		o.ID, o.OfferID, o.UserID, o.ScooterID, o.ZoneID, o.Terms.MinuteRate, o.Terms.UnlockFee, o.Terms.Deposit,
		o.Status, o.CreatedAt)
	if isUniqueViolation(err, "orders_occupied_scooter") {
		return domain.ErrScooterBusy
	}
	return err
}

// GetOrder reads a current order, falling back to the history, together with the database time.
func (q *Queries) GetOrder(ctx context.Context, id uuid.UUID) (order domain.Order, archived bool, now time.Time, err error) {
	order, err = scanOrder(q.db.QueryRow(ctx,
		`SELECT `+orderReadColumns+`, clock_timestamp() FROM orders WHERE order_id = $1`, id), &now)
	if errors.Is(err, ErrNotFound) {
		archived = true
		order, err = scanOrder(q.db.QueryRow(ctx,
			`SELECT `+orderReadColumns+`, clock_timestamp() FROM orders_history WHERE order_id = $1`, id), &now)
	}
	return order, archived, now.UTC(), err
}

// GetArchivedOrder reads an order from the history.
func (q *Queries) GetArchivedOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+` FROM orders_history WHERE order_id = $1`, id))
}

// LockOrder reads and locks a current order.
func (q *Queries) LockOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+` FROM orders WHERE order_id = $1 FOR UPDATE`, id))
}

// TryLockOrder is LockOrder that does not wait: an order locked by another
// transaction is reported as ErrNotFound.
func (q *Queries) TryLockOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+` FROM orders WHERE order_id = $1 FOR UPDATE SKIP LOCKED`, id))
}

func (q *Queries) MarkOrderActive(ctx context.Context, id uuid.UUID, startedAt time.Time) error {
	_, err := q.db.Exec(ctx, `UPDATE orders SET status = 'ACTIVE', started_at = $2 WHERE order_id = $1`, id, startedAt)
	return err
}

func (q *Queries) MarkOrderStartFailed(ctx context.Context, id uuid.UUID, closedAt, deleteAfter time.Time) error {
	_, err := q.db.Exec(ctx, `
		UPDATE orders SET status = 'START_FAILED', closed_at = $2, delete_after = $3, cost = 0
		WHERE order_id = $1`, id, closedAt, deleteAfter)
	return err
}

func (q *Queries) MarkOrderFinished(ctx context.Context, id uuid.UUID, status domain.OrderStatus, finishedAt, deleteAfter time.Time, cost *big.Int) error {
	_, err := q.db.Exec(ctx, `
		UPDATE orders SET status = $2, finish_time = $3, closed_at = $3, delete_after = $4, cost = $5
		WHERE order_id = $1`, id, status, finishedAt, deleteAfter, pgtype.Numeric{Int: cost, Valid: true})
	return err
}
