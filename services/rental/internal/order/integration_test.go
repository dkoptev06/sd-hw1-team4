package order_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/config"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/migrations"
)

func setupStore(t *testing.T) (*pgxpool.Pool, *offer.Service, *order.Service) {
	t.Helper()
	url := os.Getenv("RENTAL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set RENTAL_TEST_DATABASE_URL to run tests on PostgreSQL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{"test_" + uuid.New().String()}.Sanitize()
	execSQL(t, admin, "CREATE SCHEMA "+schema)
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	config := admin.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	sqlDB := stdlib.OpenDBFromPool(db)
	defer sqlDB.Close()
	if err := migrations.Up(ctx, sqlDB); err != nil {
		t.Fatal(err)
	}
	runner := postgres.NewTxRunner(db)
	offers := offer.NewService(db, runner)
	return db, offers, order.NewService(db, runner, offers, task.Service{})
}

func execSQL(t *testing.T, db *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func scooterSnapshot(t *testing.T, id, zoneID string, charge int) offer.ScooterSnapshot {
	t.Helper()
	snapshot, err := offer.NewScooterSnapshot(id, zoneID, charge)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func issueOffer(t *testing.T, offers *offer.Service, snapshot offer.ScooterSnapshot, deposit int64) offer.Receipt {
	t.Helper()
	quote, err := offer.NewQuote(snapshot, "user", uuid.NewString(), 120, 25, deposit, false)
	if err != nil {
		t.Fatal(err)
	}
	o, err := offers.Store(context.Background(), quote)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

type scooterSourceFunc func(context.Context, string) (offer.ScooterSnapshot, error)

func (f scooterSourceFunc) GetScooter(ctx context.Context, scooterID string) (offer.ScooterSnapshot, error) {
	return f(ctx, scooterID)
}

func sourceWithSnapshot(snapshot offer.ScooterSnapshot) scooterSourceFunc {
	return func(_ context.Context, id string) (offer.ScooterSnapshot, error) {
		if id != snapshot.ID() {
			return offer.ScooterSnapshot{}, order.ErrNotFound
		}
		return snapshot, nil
	}
}

func start(t *testing.T, store *order.Service, offerID uuid.UUID, snapshot offer.ScooterSnapshot, retention config.Retention) order.StartReceipt {
	t.Helper()
	o, err := store.StartOrder(context.Background(), offerID, sourceWithSnapshot(snapshot), retention)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

type startResponse struct {
	receipt order.StartReceipt
	err     error
}

func concurrentStarts(store *order.Service, snapshot offer.ScooterSnapshot, offerIDs ...uuid.UUID) []startResponse {
	results := make([]startResponse, len(offerIDs))
	var workers sync.WaitGroup
	barrier := make(chan struct{})
	for i, offerID := range offerIDs {
		workers.Go(func() {
			<-barrier
			receipt, err := store.StartOrder(context.Background(), offerID, sourceWithSnapshot(snapshot), config.Default().Retention())
			results[i] = startResponse{receipt, err}
		})
	}
	close(barrier)
	workers.Wait()
	return results
}

func TestConcurrentStarts(t *testing.T) {
	db, offers, store := setupStore(t)
	ctx := context.Background()
	t.Run("same offer", func(t *testing.T) {
		snapshot := scooterSnapshot(t, "same-offer", "zone", 28)
		o := issueOffer(t, offers, snapshot, 50)
		results := concurrentStarts(store, snapshot, o.OfferID, o.OfferID)
		if results[0].err != nil || results[1].err != nil {
			t.Fatalf("concurrent repeat: %v, %v", results[0].err, results[1].err)
		}
		if results[0].receipt.OrderID != results[1].receipt.OrderID ||
			results[0].receipt.Replayed == results[1].receipt.Replayed {
			t.Fatalf("expected one creation and one replay: %+v", results)
		}
		var status string
		var amount int64
		var deadline, hold int64
		err := db.QueryRow(ctx, `
			SELECT status, (parameters->>'amount')::bigint,
				extract(epoch FROM deadline_at - created_at)::bigint,
				extract(epoch FROM hold_expires_at - deadline_at)::bigint
			FROM tasks WHERE order_id = $1 AND task_type = 'START'`, results[0].receipt.OrderID).
			Scan(&status, &amount, &deadline, &hold)
		if err != nil || status != "PENDING" || amount != 50 || deadline != 120 || hold != 3600 {
			t.Fatalf("unexpected START task: %q, %d, %d, %d: %v", status, amount, deadline, hold, err)
		}
	})

	t.Run("different offers on one scooter", func(t *testing.T) {
		snapshot := scooterSnapshot(t, "same-scooter", "zone", 28)
		first := issueOffer(t, offers, snapshot, 50)
		second := issueOffer(t, offers, snapshot, 50)
		results := concurrentStarts(store, snapshot, first.OfferID, second.OfferID)
		accepted, busy := 0, 0
		for _, result := range results {
			switch {
			case result.err == nil && result.receipt.Status == order.Starting && !result.receipt.Replayed:
				accepted++
			case errors.Is(result.err, order.ErrScooterBusy):
				busy++
			default:
				t.Fatalf("unexpected start result: %+v", result)
			}
		}
		if accepted != 1 || busy != 1 {
			t.Fatalf("accepted=%d busy=%d", accepted, busy)
		}
	})
}

func TestStartReplayWhenSourceFails(t *testing.T) {
	db, offers, store := setupStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retention := config.Default().Retention()
	snapshot := scooterSnapshot(t, "scooter", "zone", 28)
	offered := issueOffer(t, offers, snapshot, 50)
	sourceError := errors.New("Source timeout")
	unavailable := scooterSourceFunc(func(context.Context, string) (offer.ScooterSnapshot, error) {
		return offer.ScooterSnapshot{}, sourceError
	})

	_, err := store.StartOrder(ctx, offered.OfferID, unavailable, retention)
	if !errors.Is(err, order.ErrSourceUnavailable) || !errors.Is(err, sourceError) {
		t.Fatalf("new start with unavailable Source: %v", err)
	}
	if _, err := store.FindOrderForOffer(ctx, offered.OfferID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("failed Source call left an order: %v", err)
	}

	var created order.StartReceipt
	failingDuringStart := scooterSourceFunc(func(ctx context.Context, id string) (offer.ScooterSnapshot, error) {
		if id != snapshot.ID() {
			t.Fatalf("Source read scooter %q, want %q", id, snapshot.ID())
		}
		receipt, err := store.StartOrder(ctx, offered.OfferID, sourceWithSnapshot(snapshot), retention)
		if err != nil || receipt.Replayed {
			t.Fatalf("start while Source request was in flight: %+v, %v", receipt, err)
		}
		created = receipt
		return offer.ScooterSnapshot{}, sourceError
	})
	replay, err := store.StartOrder(ctx, offered.OfferID, failingDuringStart, retention)
	if err != nil || !replay.Replayed || replay.OrderID != created.OrderID {
		t.Fatalf("concurrent replay: %+v, %v", replay, err)
	}

	noSource := scooterSourceFunc(func(context.Context, string) (offer.ScooterSnapshot, error) {
		t.Fatal("Source was called for a replay or unavailable offer")
		return offer.ScooterSnapshot{}, nil
	})
	replay, err = store.StartOrder(ctx, offered.OfferID, noSource, retention)
	if err != nil || !replay.Replayed || replay.OrderID != created.OrderID {
		t.Fatalf("replay of existing order: %+v, %v", replay, err)
	}
	if _, err := store.StartOrder(ctx, uuid.New(), noSource, retention); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("unknown offer: %v", err)
	}

	expired := issueOffer(t, offers, snapshot, 50)
	execSQL(t, db, "UPDATE offers SET expires_at = clock_timestamp() WHERE offer_id = $1", expired.OfferID)
	if _, err := store.StartOrder(ctx, expired.OfferID, noSource, retention); !errors.Is(err, offer.ErrExpired) {
		t.Fatalf("expired offer: %v", err)
	}
}

func TestExhaustStart(t *testing.T) {
	db, offers, store := setupStore(t)
	ctx := context.Background()
	retention := config.Default().Retention()
	const maxAttempts = 3

	for _, test := range []struct {
		name      string
		attempts  int
		leaseLive bool
		overdue   bool
		deposit   int64
		reason    string
		voids     int
	}{
		{name: "another attempt remains", attempts: maxAttempts - 1, deposit: 50},
		{name: "last attempt can succeed", attempts: maxAttempts, leaseLive: true, deposit: 50},
		{name: "last lease expired", attempts: maxAttempts, deposit: 50, reason: "MAX_ATTEMPTS_EXCEEDED", voids: 1},
		{name: "deadline overrides active lease", attempts: maxAttempts, leaseLive: true, overdue: true, reason: "DEADLINE_EXCEEDED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := scooterSnapshot(t, uuid.NewString(), "zone", 28)
			created := start(t, store, issueOffer(t, offers, snapshot, test.deposit).OfferID, snapshot, retention)
			claimID := uuid.New()
			execSQL(t, db, `
				UPDATE tasks
				SET attempt_count = $2, claim_id = $3,
					lease_until = CASE WHEN $4::bool THEN clock_timestamp() + interval '30 seconds'
						ELSE clock_timestamp() - interval '1 second' END,
					deadline_at = CASE WHEN $5::bool THEN clock_timestamp() - interval '1 second' ELSE deadline_at END,
					next_run_at = clock_timestamp() + interval '1 day'
				WHERE order_id = $1 AND task_type = 'START'`, created.OrderID, test.attempts, claimID, test.leaseLive, test.overdue)

			wantFinalized := test.reason != ""
			finalized, err := store.ExhaustStart(ctx, created.OrderID, maxAttempts, retention)
			if err != nil || finalized != wantFinalized {
				t.Fatalf("finalized=%t, want %t: %v", finalized, wantFinalized, err)
			}
			wantOrderStatus, wantTaskStatus := order.Starting, "PENDING"
			if wantFinalized {
				wantOrderStatus, wantTaskStatus = order.StartFailed, "COMPLETED"
				if finalized, err := store.ExhaustStart(ctx, created.OrderID, maxAttempts, retention); err != nil || finalized {
					t.Fatalf("repeated exhaustion: finalized=%t, err=%v", finalized, err)
				}
			}
			if test.leaseLive {
				applied, err := store.ApplyStart(ctx, created.OrderID, claimID, order.StartSucceeded, retention)
				if err != nil || applied == wantFinalized {
					t.Fatalf("last attempt applied=%t, err=%v", applied, err)
				}
				if applied {
					wantOrderStatus, wantTaskStatus = order.Active, "COMPLETED"
				}
			}
			view, err := store.GetOrder(ctx, created.OrderID)
			if err != nil || view.Status != wantOrderStatus {
				t.Fatalf("order status=%s, want %s: %v", view.Status, wantOrderStatus, err)
			}

			var status, reason string
			var attempts, voidCount int
			var claimCleared bool
			err = db.QueryRow(ctx, `
				SELECT status, COALESCE(completion_reason, ''), attempt_count, claim_id IS NULL,
					(SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID')
				FROM tasks WHERE order_id = $1 AND task_type = 'START'`, created.OrderID).
				Scan(&status, &reason, &attempts, &claimCleared, &voidCount)
			if err != nil || status != wantTaskStatus || reason != test.reason || attempts != test.attempts ||
				claimCleared != (wantTaskStatus == "COMPLETED") || voidCount != test.voids {
				t.Fatalf("task status=%s reason=%q attempts=%d claim cleared=%t VOID count=%d: %v",
					status, reason, attempts, claimCleared, voidCount, err)
			}
		})
	}
}

func TestExpiredStartReplacement(t *testing.T) {
	db, offers, store := setupStore(t)
	ctx := context.Background()
	retention, err := config.NewRetention(365, map[string]int{"old-zone": 30})
	if err != nil {
		t.Fatal(err)
	}

	old := scooterSnapshot(t, "scooter", "old-zone", 28)
	previous := start(t, store, issueOffer(t, offers, old, 50).OfferID, old, retention)
	claimID := uuid.New()
	execSQL(t, db, `
		UPDATE tasks
		SET deadline_at = clock_timestamp() - interval '1 second',
			claim_id = $2, lease_until = clock_timestamp() + interval '1 day',
			next_run_at = clock_timestamp() + interval '1 day', attempt_count = 1
		WHERE order_id = $1 AND task_type = 'START'`, previous.OrderID, claimID)

	updated := scooterSnapshot(t, old.ID(), "new-zone", 55)
	replacement := start(t, store, issueOffer(t, offers, updated, 50).OfferID, updated, retention)
	oldOrder, err := store.GetOrder(ctx, previous.OrderID)
	if err != nil || oldOrder.Status != order.StartFailed || oldOrder.Cost == nil || *oldOrder.Cost != 0 ||
		replacement.Status != order.Starting {
		t.Fatalf("unexpected replacement: previous=%+v replacement=%+v err=%v", oldOrder, replacement, err)
	}
	var days, voidCount int
	var taskStatus string
	err = db.QueryRow(ctx, `
		SELECT extract(day FROM o.delete_after - o.closed_at)::int, t.status,
			(SELECT count(*) FROM tasks WHERE order_id = o.order_id AND task_type = 'VOID')
		FROM orders o JOIN tasks t ON t.order_id = o.order_id AND t.task_type = 'START'
		WHERE o.order_id = $1`, previous.OrderID).Scan(&days, &taskStatus, &voidCount)
	if err != nil || days != 30 || taskStatus != "PENDING" || voidCount != 0 {
		t.Fatalf("API must only close the old order: %d %q %d: %v", days, taskStatus, voidCount, err)
	}
	if applied, err := store.ApplyStart(ctx, previous.OrderID, claimID, order.StartSucceeded, retention); applied || err != nil {
		t.Fatalf("late result applied=%t err=%v", applied, err)
	}
	if expired, err := store.ExpireStart(ctx, previous.OrderID, retention); !expired || err != nil {
		t.Fatalf("expire result=%t err=%v", expired, err)
	}
	if expired, err := store.ExpireStart(ctx, previous.OrderID, retention); expired || err != nil {
		t.Fatalf("repeat expire result=%t err=%v", expired, err)
	}
	var reason string
	var claimCleared bool
	err = db.QueryRow(ctx, `
		SELECT status, completion_reason, claim_id IS NULL,
			(SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID' AND parameters->>'operation_id' = $1::uuid::text)
		FROM tasks WHERE order_id = $1 AND task_type = 'START'`, previous.OrderID).
		Scan(&taskStatus, &reason, &claimCleared, &voidCount)
	if err != nil || taskStatus != "COMPLETED" || reason != "DEADLINE_EXCEEDED" || !claimCleared || voidCount != 1 {
		t.Fatalf("expired task: %q %q %t %d: %v", taskStatus, reason, claimCleared, voidCount, err)
	}
}

func TestFinishAndArchiveRetry(t *testing.T) {
	db, offers, store := setupStore(t)
	ctx := context.Background()
	retention := config.Default().Retention()
	snapshot := scooterSnapshot(t, "scooter", "zone", 28)
	o := issueOffer(t, offers, snapshot, 0)
	created := start(t, store, o.OfferID, snapshot, retention)
	if _, err := store.FinishOrder(ctx, created.OrderID, retention); !errors.Is(err, order.ErrInactive) {
		t.Fatalf("finish while STARTING: %v", err)
	}
	claimID := uuid.New()
	execSQL(t, db, `
		UPDATE tasks SET claim_id = $2, lease_until = deadline_at, attempt_count = 1
		WHERE order_id = $1 AND task_type = 'START'`, created.OrderID, claimID)
	if applied, err := store.ApplyStart(ctx, created.OrderID, claimID, order.StartSucceeded, retention); !applied || err != nil {
		t.Fatalf("activation applied=%t err=%v", applied, err)
	}
	execSQL(t, db, `
		UPDATE orders SET started_at = clock_timestamp() - interval '6 seconds' WHERE order_id = $1`, created.OrderID)
	view, err := store.GetOrder(ctx, created.OrderID)
	if err != nil || view.Status != order.Active || view.Cost == nil || *view.Cost < 25 {
		t.Fatalf("active cost=%+v err=%v", view, err)
	}
	closed, err := store.FinishOrder(ctx, created.OrderID, retention)
	if err != nil || closed.Status != order.Completed || closed.Cost == nil || *closed.Cost < *view.Cost {
		t.Fatalf("finish=%+v err=%v", closed, err)
	}
	var settledAmount int64
	err = db.QueryRow(ctx, `SELECT (parameters->>'total_amount')::bigint
		FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE'`, created.OrderID).Scan(&settledAmount)
	if err != nil || settledAmount != *closed.Cost {
		t.Fatalf("SETTLE amount=%d err=%v", settledAmount, err)
	}
	var expiry time.Time
	err = db.QueryRow(ctx, "SELECT delete_after FROM orders WHERE order_id = $1", created.OrderID).Scan(&expiry)
	if err != nil {
		t.Fatal(err)
	}
	day := expiry.UTC().Truncate(24 * time.Hour)
	monday := day.AddDate(0, 0, -(int(day.Weekday())+6)%7)
	end := monday.AddDate(0, 0, 7)
	execSQL(t, db, fmt.Sprintf("CREATE TABLE history_week PARTITION OF orders_history FOR VALUES FROM ('%s') TO ('%s')",
		monday.Format(time.RFC3339), end.Format(time.RFC3339)))
	// Only the fixture moves the row here. This exercises the real lookup on both
	// sides of a transfer without a dependency on the maintenance implementation.
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders_history
			SELECT o.order_id, o.offer_id, o.user_id, o.scooter_id, o.zone_id, o.minute_rate, o.unlock_fee, o.deposit,
				f.user_profile_fallback_used, o.status, o.created_at, o.started_at, o.finish_time, o.closed_at,
				o.delete_after, o.cost
			FROM orders o JOIN offers f USING (offer_id) WHERE o.order_id = $1`, created.OrderID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM tasks WHERE order_id = $1", created.OrderID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM orders WHERE order_id = $1", created.OrderID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM offers WHERE offer_id = $1", o.OfferID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.FinishOrder(ctx, created.OrderID, retention)
	if err != nil || replay.Status != closed.Status || replay.Cost == nil || *replay.Cost != *closed.Cost ||
		replay.FinishTime == nil || !replay.FinishTime.Equal(*closed.FinishTime) {
		t.Fatalf("historic finish=%+v err=%v", replay, err)
	}
	replayedStart, err := store.FindOrderForOffer(ctx, o.OfferID)
	if err != nil || !replayedStart.Replayed || replayedStart.OrderID != created.OrderID {
		t.Fatalf("historic start=%+v err=%v", replayedStart, err)
	}
}

func TestStartRollsBackWhenTaskInsertFails(t *testing.T) {
	db, offers, orders := setupStore(t)
	ctx := context.Background()
	current := scooterSnapshot(t, "rollback-scooter", "zone", 30)
	quote := issueOffer(t, offers, current, 50)
	retention := config.Default().Retention()

	execSQL(t, db, `ALTER TABLE tasks ADD CONSTRAINT test_reject_start CHECK (task_type <> 'START')`)
	_, err := orders.StartOrder(ctx, quote.OfferID, sourceWithSnapshot(current), retention)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "test_reject_start" {
		t.Fatalf("expected the START insert to fail, got %v", err)
	}

	var status string
	if err := db.QueryRow(ctx, "SELECT status FROM offers WHERE offer_id = $1", quote.OfferID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "AVAILABLE" {
		t.Fatalf("offer status after rollback is %s", status)
	}
	if _, err := orders.FindOrderForOffer(ctx, quote.OfferID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("order survived rollback: %v", err)
	}

	execSQL(t, db, `ALTER TABLE tasks DROP CONSTRAINT test_reject_start`)
	created := start(t, orders, quote.OfferID, current, retention)
	if created.Replayed || created.Status != order.Starting {
		t.Fatalf("retry after rollback did not create an order: %+v", created)
	}
}
