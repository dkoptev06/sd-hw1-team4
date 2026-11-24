package order

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

// Both orders and orders_history use the same order columns.
const orderColumnsBeforeCost = `order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
	status, created_at, started_at, finish_time, closed_at, delete_after`
const orderReadColumns = orderColumnsBeforeCost + `, cost::text`

type orderRow struct {
	id, offerID                                  uuid.UUID
	userID, scooterID, zoneID                    string
	minuteRate, unlockFee, deposit               int64
	status                                       string
	createdAt                                    time.Time
	startedAt, finishTime, closedAt, deleteAfter *time.Time
	cost                                         *string
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	value := t.UTC()
	return &value
}

func scanOrder(row pgx.Row, extra ...any) (trip, error) {
	var r orderRow
	dest := []any{&r.id, &r.offerID, &r.userID, &r.scooterID, &r.zoneID, &r.minuteRate, &r.unlockFee,
		&r.deposit, &r.status, &r.createdAt, &r.startedAt, &r.finishTime, &r.closedAt, &r.deleteAfter, &r.cost}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return trip{}, err
	}
	return r.model()
}

func (r orderRow) model() (trip, error) {
	var cost *big.Int
	if r.cost != nil {
		var ok bool
		cost, ok = new(big.Int).SetString(*r.cost, 10)
		if !ok {
			return trip{}, fmt.Errorf("invalid stored cost for order %s: %q", r.id, *r.cost)
		}
	}
	terms := offer.Terms{MinuteRate: r.minuteRate, UnlockFee: r.unlockFee, Deposit: r.deposit}
	o, err := restoreTrip(r.id, r.offerID, r.userID, r.scooterID, r.zoneID, terms, state{
		status: Status(r.status), createdAt: r.createdAt.UTC(), startedAt: utc(r.startedAt), finishTime: utc(r.finishTime),
		closedAt: utc(r.closedAt), deleteAfter: utc(r.deleteAfter), cost: cost,
	})
	if err != nil {
		return trip{}, fmt.Errorf("restore order %s: %w", r.id, err)
	}
	return o, nil
}

type repository struct{ db postgres.DBTX }

// findOrderByOffer reads the current order or its archived record using the
// partition-pruned history locator so live starts never scan all history partitions.
func (q repository) findOrderByOffer(ctx context.Context, offerID uuid.UUID) (StartResult, error) {
	var ref StartResult
	err := q.db.QueryRow(ctx, `SELECT order_id, status FROM orders WHERE offer_id = $1`, offerID).Scan(&ref.OrderID, &ref.Status)
	if !errors.Is(err, pgx.ErrNoRows) {
		return ref, err
	}
	var orderID uuid.UUID
	var deleteAfter time.Time
	err = q.db.QueryRow(ctx,
		`SELECT order_id, delete_after FROM order_history_locator WHERE offer_id = $1`, offerID).Scan(&orderID, &deleteAfter)
	if err != nil {
		return StartResult{}, err
	}
	err = q.db.QueryRow(ctx,
		`SELECT order_id, status`+historyByID,
		pgx.QueryExecModeExec, orderID, deleteAfter).Scan(&ref.OrderID, &ref.Status)
	return ref, err
}

// lockScooterForStart serializes starts for a scooter even when it has no order yet.
// The lock is held until the start transaction ends.
func (q repository) lockScooterForStart(ctx context.Context, scooterID string) error {
	_, err := q.db.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 1))", scooterID)
	return err
}

// lockOccupyingOrder locks the STARTING or ACTIVE order of a scooter, if any, with the
// deadline of its START task.
func (q repository) lockOccupyingOrder(ctx context.Context, scooterID string) (trip, *time.Time, error) {
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

// insertOrder stores a new order. A concurrent start on the same scooter violates
// the one-active-order index and is reported as SCOOTER_BUSY.
func (q repository) insertOrder(ctx context.Context, o trip) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
			status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		o.id, o.offerID, o.userID, o.scooterID, o.zoneID, o.terms.MinuteRate, o.terms.UnlockFee, o.terms.Deposit,
		o.status, o.createdAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "orders_occupied_scooter" {
		return ErrScooterBusy
	}
	return err
}

// Use a custom plan with the bound partition key. A cached generic plan can
// lock partitions before pruning them at execution time.
const historyByID = ` FROM orders_history WHERE order_id = $1 AND delete_after = $2`

func (q repository) historyDeadline(ctx context.Context, id uuid.UUID) (time.Time, error) {
	var deadline time.Time
	err := q.db.QueryRow(ctx, `SELECT delete_after FROM order_history_locator WHERE order_id = $1`, id).Scan(&deadline)
	return deadline, err
}

// getOrder reads current before history so concurrent archiving stays visible.
func (q repository) getOrder(ctx context.Context, id uuid.UUID) (trip, bool, time.Time, error) {
	var now time.Time
	order, err := scanOrder(q.db.QueryRow(ctx,
		`SELECT `+orderReadColumns+`, clock_timestamp() FROM orders WHERE order_id = $1`, id), &now)
	if !errors.Is(err, pgx.ErrNoRows) {
		return order, false, now.UTC(), err
	}
	deadline, err := q.historyDeadline(ctx, id)
	if err != nil {
		return trip{}, false, time.Time{}, err
	}
	order, err = scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+`, clock_timestamp()`+historyByID,
		pgx.QueryExecModeExec, id, deadline), &now)
	return order, true, now.UTC(), err
}

// getArchivedOrder reads an order from a single pruned history partition.
func (q repository) getArchivedOrder(ctx context.Context, id uuid.UUID) (trip, error) {
	deadline, err := q.historyDeadline(ctx, id)
	if err != nil {
		return trip{}, err
	}
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+historyByID, pgx.QueryExecModeExec, id, deadline))
}

// lockOrder reads and locks a current order.
func (q repository) lockOrder(ctx context.Context, id uuid.UUID) (trip, error) {
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+` FROM orders WHERE order_id = $1 FOR UPDATE`, id))
}

// tryLockOrder is lockOrder that does not wait: an order locked by another
// transaction is reported as pgx.ErrNoRows.
func (q repository) tryLockOrder(ctx context.Context, id uuid.UUID) (trip, error) {
	return scanOrder(q.db.QueryRow(ctx, `SELECT `+orderReadColumns+` FROM orders WHERE order_id = $1 FOR UPDATE SKIP LOCKED`, id))
}

func (q repository) markOrderActive(ctx context.Context, id uuid.UUID, startedAt time.Time) error {
	_, err := q.db.Exec(ctx, `UPDATE orders SET status = 'ACTIVE', started_at = $2 WHERE order_id = $1`, id, startedAt)
	return err
}

func (q repository) markOrderStartFailed(ctx context.Context, id uuid.UUID, closedAt, deleteAfter time.Time) error {
	_, err := q.db.Exec(ctx, `
		UPDATE orders SET status = 'START_FAILED', closed_at = $2, delete_after = $3, cost = 0
		WHERE order_id = $1`, id, closedAt, deleteAfter)
	return err
}

func (q repository) markOrderFinished(ctx context.Context, id uuid.UUID, status Status, finishedAt, deleteAfter time.Time, cost *big.Int) error {
	_, err := q.db.Exec(ctx, `
		UPDATE orders SET status = $2, finish_time = $3, closed_at = $3, delete_after = $4, cost = $5
		WHERE order_id = $1`, id, status, finishedAt, deleteAfter, pgtype.Numeric{Int: cost, Valid: true})
	return err
}
