package order

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

func findOrderForOffer(ctx context.Context, db postgres.DBTX, offerID uuid.UUID) (StartReceipt, error) {
	var receipt StartReceipt
	err := db.QueryRow(ctx, `SELECT order_id, status FROM orders WHERE offer_id = $1`, offerID).
		Scan(&receipt.OrderID, &receipt.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		err = db.QueryRow(ctx, `SELECT order_id, status FROM orders_history WHERE offer_id = $1`, offerID).
			Scan(&receipt.OrderID, &receipt.Status)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return StartReceipt{}, ErrNotFound
	}
	receipt.Replayed = err == nil
	return receipt, err
}

func lockScooter(ctx context.Context, tx postgres.Tx, scooterID string) error {
	// Serialize the empty-slot case as well as an occupied scooter. The partial
	// unique index enforces the same rule for other database writers.
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 1))", scooterID)
	return err
}

type occupiedOrder struct {
	id     uuid.UUID
	status Status
	zoneID string
}

func lockOccupiedOrder(ctx context.Context, tx postgres.Tx, scooterID string) (occupiedOrder, error) {
	var order occupiedOrder
	err := tx.QueryRow(ctx, `
		SELECT order_id, status, zone_id FROM orders
		WHERE scooter_id = $1 AND status IN ('STARTING', 'ACTIVE') FOR UPDATE`, scooterID).
		Scan(&order.id, &order.status, &order.zoneID)
	if errors.Is(err, pgx.ErrNoRows) {
		return occupiedOrder{}, nil
	}
	return order, err
}

func insertOrder(ctx context.Context, tx postgres.Tx, orderID, offerID uuid.UUID, snapshot offer.StartSnapshot, createdAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'STARTING', $9)`,
		orderID, offerID, snapshot.UserID(), snapshot.ScooterID(), snapshot.ZoneID(),
		snapshot.MinuteRate(), snapshot.UnlockFee(), snapshot.Deposit(), createdAt)
	return err
}

func databaseTime(ctx context.Context, db postgres.DBTX) (time.Time, error) {
	var now time.Time
	err := db.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now.UTC(), err
}

func markStartFailed(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, closedAt, deleteAfter time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'START_FAILED', closed_at = $2, delete_after = $3, cost = 0
		WHERE order_id = $1`, orderID, closedAt, deleteAfter)
	return err
}

func getOrder(ctx context.Context, db postgres.DBTX, orderID uuid.UUID) (View, error) {
	var view View
	err := db.QueryRow(ctx, `
		SELECT order_id, status, finish_time,
			CASE WHEN status = 'ACTIVE' THEN rental_trip_cost(minute_rate, unlock_fee, clock_timestamp() - started_at)
				ELSE cost END, started_at
		FROM orders WHERE order_id = $1`, orderID).Scan(
		&view.OrderID, &view.Status, &view.FinishTime, &view.Cost, &view.StartedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = db.QueryRow(ctx, `
			SELECT order_id, status, finish_time, cost, started_at FROM orders_history WHERE order_id = $1`, orderID).Scan(
			&view.OrderID, &view.Status, &view.FinishTime, &view.Cost, &view.StartedAt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrNotFound
	}
	view.FinishTime = utc(view.FinishTime)
	view.StartedAt = utc(view.StartedAt)
	return view, err
}

func utc(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	converted := value.UTC()
	return &converted
}

type finishState struct {
	receipt               FinishReceipt
	userID, zoneID        string
	startedAt             *time.Time
	minuteRate, unlockFee int64
}

func lockOrderForFinish(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (finishState, error) {
	state := finishState{receipt: FinishReceipt{OrderID: orderID}}
	err := tx.QueryRow(ctx, `
		SELECT status, user_id, zone_id, started_at, minute_rate, unlock_fee, finish_time, cost
		FROM orders WHERE order_id = $1 FOR UPDATE`, orderID).Scan(
		&state.receipt.Status, &state.userID, &state.zoneID, &state.startedAt, &state.minuteRate,
		&state.unlockFee, &state.receipt.FinishTime, &state.receipt.Cost)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			SELECT status, finish_time, cost FROM orders_history WHERE order_id = $1`, orderID).
			Scan(&state.receipt.Status, &state.receipt.FinishTime, &state.receipt.Cost)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return finishState{}, ErrNotFound
	}
	state.receipt.FinishTime = utc(state.receipt.FinishTime)
	return state, err
}

func tripCost(ctx context.Context, tx postgres.Tx, minuteRate, unlockFee int64, finishedAt, startedAt time.Time) (int64, error) {
	var cost int64
	err := tx.QueryRow(ctx, "SELECT rental_trip_cost($1, $2, $3::timestamptz - $4::timestamptz)",
		minuteRate, unlockFee, finishedAt, startedAt).Scan(&cost)
	return cost, err
}

func markFinished(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, status Status, finishedAt, deleteAfter time.Time, cost int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders SET status = $2, finish_time = $3, closed_at = $3, delete_after = $4, cost = $5
		WHERE order_id = $1`, orderID, status, finishedAt, deleteAfter, cost)
	return err
}

type startingOrder struct {
	status  Status
	zoneID  string
	deposit int64
}

func lockOrderForStartResult(ctx context.Context, tx postgres.Tx, orderID uuid.UUID) (startingOrder, error) {
	var order startingOrder
	err := tx.QueryRow(ctx, `
		SELECT status, zone_id, deposit FROM orders WHERE order_id = $1 FOR UPDATE`, orderID).
		Scan(&order.status, &order.zoneID, &order.deposit)
	return order, err
}

func markActive(ctx context.Context, tx postgres.Tx, orderID uuid.UUID, startedAt time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'ACTIVE', started_at = $2 WHERE order_id = $1`, orderID, startedAt)
	return err
}
