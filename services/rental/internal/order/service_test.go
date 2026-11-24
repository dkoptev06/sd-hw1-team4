package order

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// scooterBoard is a mutable fake of the scooters source.
type scooterBoard struct {
	mu       sync.Mutex
	scooters map[string]source.Scooter
	err      error
}

func (b *scooterBoard) Scooter(_ context.Context, id string) (source.Scooter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return source.Scooter{}, b.err
	}
	s, ok := b.scooters[id]
	if !ok {
		return source.Scooter{}, source.ErrNotFound
	}
	return s, nil
}

func (b *scooterBoard) set(s source.Scooter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scooters[s.ID] = s
}

type scootersFunc func(context.Context, string) (source.Scooter, error)

func (f scootersFunc) Scooter(ctx context.Context, id string) (source.Scooter, error) {
	return f(ctx, id)
}

type pricingStub struct{ deposit *int64 }

func (p pricingStub) Tariff(_ context.Context, zoneID string) (source.Tariff, error) {
	return source.Tariff{ZoneID: zoneID, PricePerMinute: 60, PriceUnlock: 50, DefaultDeposit: *p.deposit}, nil
}

func (pricingStub) UserProfile(_ context.Context, id string) (source.UserProfile, error) {
	return source.UserProfile{UserID: id}, nil
}

type staticConfigs struct{ snapshot dynconfig.Snapshot }

func (c staticConfigs) Current() dynconfig.Snapshot { return c.snapshot }

type orderEnv struct {
	store    *pgxpool.Pool
	offers   *offer.Service
	orders   *Service
	scooters *scooterBoard
	deposit  int64
}

func newOrderEnv(t *testing.T) *orderEnv {
	t.Helper()
	store := tests.NewDB(t)
	retention, err := dynconfig.NewRetention(365, map[string]int{"zone-2": 30})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := dynconfig.Defaults()
	snapshot.Retention = retention
	configs := staticConfigs{snapshot}

	env := &orderEnv{store: store, scooters: &scooterBoard{scooters: map[string]source.Scooter{}}, deposit: 300}
	pricing := pricingStub{deposit: &env.deposit}
	env.offers = offer.NewService(store, env.scooters, pricing, pricing, configs, 2*time.Minute)
	env.orders = NewService(store, env.offers, task.NewService(store, task.Options{}), env.scooters, configs, 2*time.Minute, time.Hour, slog.Default())
	return env
}

func (e *orderEnv) offer(t *testing.T, scooterID, zoneID string) offer.Receipt {
	t.Helper()
	e.scooters.set(source.Scooter{ID: scooterID, ZoneID: zoneID, Charge: 80})
	offer, _, err := e.offers.Create(context.Background(), scooterID, "user", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return offer
}

func (e *orderEnv) start(t *testing.T, offerID uuid.UUID) StartResult {
	t.Helper()
	result, err := e.orders.Start(context.Background(), offerID)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (e *orderEnv) activate(t *testing.T, orderID uuid.UUID, ago time.Duration) {
	t.Helper()
	tests.ExecSQL(t, e.store, `
		UPDATE orders SET status = 'ACTIVE', started_at = clock_timestamp() - make_interval(secs => $2)
		WHERE order_id = $1`, orderID, ago.Seconds())
	tests.ExecSQL(t, e.store, `
		UPDATE tasks SET status = 'COMPLETED', finished_at = clock_timestamp(), result = 'success'
		WHERE order_id = $1 AND task_type = 'START'`, orderID)
}

func TestStartCreatesOrderAndTask(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offer := env.offer(t, "s", "zone-1")
	result := env.start(t, offer.ID)
	if result.Replayed || result.Status != Starting {
		t.Fatalf("start: %+v", result)
	}

	var params struct {
		OperationID   string    `json:"operation_id"`
		UserID        string    `json:"user_id"`
		Amount        int64     `json:"amount"`
		HoldExpiresAt time.Time `json:"hold_expires_at"`
	}
	var deadline, created time.Time
	err := env.store.QueryRow(ctx, `
		SELECT parameters, deadline_at, created_at FROM tasks WHERE order_id = $1 AND task_type = 'START'`,
		result.OrderID).Scan(&params, &deadline, &created)
	if err != nil {
		t.Fatal(err)
	}
	if params.OperationID != result.OrderID.String() || params.UserID != "user" || params.Amount != 300 ||
		deadline.Sub(created) != 2*time.Minute || !params.HoldExpiresAt.Equal(deadline.Add(time.Hour)) {
		t.Fatalf("START params %+v deadline %s created %s", params, deadline, created)
	}

	view, err := env.orders.Get(ctx, result.OrderID)
	if err != nil || view.Status != Starting || view.Cost != nil {
		t.Fatalf("view %+v err=%v", view, err)
	}

	stored, _, _, err := env.orders.repo.getOrder(ctx, result.OrderID)
	if err != nil || stored.terms != offer.Terms {
		t.Fatalf("stored terms %+v err=%v", stored.terms, err)
	}

	replay := env.start(t, offer.ID)
	if !replay.Replayed || replay.OrderID != result.OrderID {
		t.Fatalf("replay: %+v", replay)
	}
}

func TestConcurrentStarts(t *testing.T) {
	env := newOrderEnv(t)
	concurrently := func(offerIDs ...uuid.UUID) ([]StartResult, []error) {
		results, errs := make([]StartResult, len(offerIDs)), make([]error, len(offerIDs))
		var wg sync.WaitGroup
		gate := make(chan struct{})
		for i, id := range offerIDs {
			wg.Go(func() {
				<-gate
				results[i], errs[i] = env.orders.Start(context.Background(), id)
			})
		}
		close(gate)
		wg.Wait()
		return results, errs
	}

	t.Run("same offer", func(t *testing.T) {
		offer := env.offer(t, "same-offer", "zone-1")
		results, errs := concurrently(offer.ID, offer.ID, offer.ID)
		created := 0
		for i, r := range results {
			if errs[i] != nil || r.OrderID != results[0].OrderID {
				t.Fatalf("results %+v errors %v", results, errs)
			}
			if !r.Replayed {
				created++
			}
		}
		if created != 1 {
			t.Fatalf("created %d orders", created)
		}
	})

	t.Run("offers of one scooter", func(t *testing.T) {
		first, second := env.offer(t, "same-scooter", "zone-1"), env.offer(t, "same-scooter", "zone-1")
		results, errs := concurrently(first.ID, second.ID)
		accepted, busy := 0, 0
		for i := range results {
			switch {
			case errs[i] == nil && !results[i].Replayed:
				accepted++
			case errors.Is(errs[i], ErrScooterBusy):
				busy++
			}
		}
		if accepted != 1 || busy != 1 {
			t.Fatalf("accepted=%d busy=%d errors=%v", accepted, busy, errs)
		}
	})
}

func TestStartRejections(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()

	if _, err := env.orders.Start(ctx, uuid.New()); !errors.Is(err, offer.ErrNotFound) {
		t.Fatalf("unknown offer: %v", err)
	}

	expired := env.offer(t, "s-expired", "zone-1")
	tests.ExecSQL(t, env.store, `UPDATE offers SET expires_at = clock_timestamp() WHERE offer_id = $1`, expired.ID)
	if _, err := env.orders.Start(ctx, expired.ID); !errors.Is(err, offer.ErrOfferExpired) {
		t.Fatalf("expired offer: %v", err)
	}

	moved := env.offer(t, "s-moved", "zone-1")
	env.scooters.set(source.Scooter{ID: "s-moved", ZoneID: "zone-2", Charge: 80})
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, offer.ErrOfferChanged) {
		t.Fatalf("zone changed: %v", err)
	}
	env.scooters.set(source.Scooter{ID: "s-moved", ZoneID: "zone-1", Charge: 27})
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, offer.ErrOfferChanged) {
		t.Fatalf("charge crossed threshold: %v", err)
	}
	env.scooters.set(source.Scooter{ID: "s-moved", ZoneID: "zone-1", Charge: 40})
	busyOffer := env.offer(t, "s-moved", "zone-1")

	env.scooters.err = fmt.Errorf("%w: timeout", source.ErrUnavailable)
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("Source down: %v", err)
	}
	if _, err := env.orders.repo.findOrderByOffer(ctx, moved.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("failed start left an order: %v", err)
	}
	env.scooters.err = nil

	// The charge changed within the same category: the start is allowed.
	env.start(t, moved.ID)
	if _, err := env.orders.Start(ctx, busyOffer.ID); !errors.Is(err, ErrScooterBusy) {
		t.Fatalf("busy scooter: %v", err)
	}

	// A replay is answered from the order even when Source is down.
	env.scooters.err = source.ErrUnavailable
	if replay, err := env.orders.Start(ctx, moved.ID); err != nil || !replay.Replayed {
		t.Fatalf("replay with Source down: %+v %v", replay, err)
	}
}

func TestExpiredStartIsReplaced(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	previous := env.start(t, env.offer(t, "s", "zone-2").ID)
	tests.ExecSQL(t, env.store, `
		UPDATE tasks SET deadline_at = clock_timestamp() - interval '1 second'
		WHERE order_id = $1 AND task_type = 'START'`, previous.OrderID)

	replacement := env.start(t, env.offer(t, "s", "zone-2").ID)
	if replacement.Status != Starting {
		t.Fatalf("replacement %+v", replacement)
	}
	old, err := env.orders.Get(ctx, previous.OrderID)
	if err != nil || old.Status != StartFailed || old.Cost == nil || old.Cost.Sign() != 0 {
		t.Fatalf("previous order %+v err=%v", old, err)
	}
	stored, _, _, err := env.orders.repo.getOrder(ctx, previous.OrderID)
	if err != nil || stored.deleteAfter == nil || stored.closedAt == nil {
		t.Fatalf("closed order %+v err=%v", stored, err)
	}
	if days := stored.deleteAfter.Sub(*stored.closedAt).Hours() / 24; days != 30 {
		t.Fatalf("zone retention %v days", days)
	}
	var taskStatus string
	var voids int
	err = env.store.QueryRow(ctx, `
		SELECT status, (SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID')
		FROM tasks WHERE order_id = $1 AND task_type = 'START'`, previous.OrderID).Scan(&taskStatus, &voids)
	if err != nil || taskStatus != "PENDING" || voids != 0 {
		t.Fatalf("API must leave the START task to the worker: %s voids=%d err=%v", taskStatus, voids, err)
	}
}

func TestStartRejectsOfferThatExpiresWhileWaitingForOrderLock(t *testing.T) {
	env := newOrderEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	previous := env.start(t, env.offer(t, "scooter", "zone-1").ID)
	next := env.offer(t, "scooter", "zone-1")
	tests.ExecSQL(t, env.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, previous.OrderID)

	tx, err := env.store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var lockerPID int32
	err = tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM orders WHERE order_id = $1 FOR UPDATE`, previous.OrderID).Scan(&lockerPID)
	if err != nil {
		t.Fatal(err)
	}
	var expiresAt time.Time
	err = env.store.QueryRow(ctx, `
		UPDATE offers SET expires_at = clock_timestamp() + interval '2 seconds'
		WHERE offer_id = $1 RETURNING expires_at`, next.ID).Scan(&expiresAt)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := env.orders.Start(ctx, next.ID)
		result <- err
	}()

	// Observe the lock wait before letting the offer expire.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := env.store.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid)))`, lockerPID).
			Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("start returned without waiting for the order lock: %v", err)
		case <-ctx.Done():
			t.Fatal("start did not wait for the order lock")
		case <-ticker.C:
		}
	}

	if _, err := env.store.Exec(ctx, `SELECT pg_sleep_until($1)`, expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, offer.ErrOfferExpired) {
		t.Fatalf("start after waiting past the offer expiration: %v", err)
	}
	env.requireAvailableOffer(t, ctx, next.ID, "offer changed after rejected start")
	if _, err := env.orders.repo.findOrderByOffer(ctx, next.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("rejected start left an order: %v", err)
	}
	old, err := env.orders.Get(ctx, previous.OrderID)
	if err != nil || old.Status != Starting {
		t.Fatalf("rejected start changed previous order: %+v, %v", old, err)
	}
}

func TestFinish(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	started := env.start(t, env.offer(t, "s", "zone-1").ID)
	if _, err := env.orders.Finish(ctx, started.OrderID); !errors.Is(err, ErrOrderNotActive) {
		t.Fatalf("finish STARTING: %v", err)
	}
	if _, err := env.orders.Finish(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("finish unknown: %v", err)
	}

	env.activate(t, started.OrderID, 61*time.Second)
	view, err := env.orders.Get(ctx, started.OrderID)
	// minute rate floor(60 × 1.25) = 75; cost 50 + floor(61 × 75 / 60) = 126; the read may run a second later.
	if err != nil || view.Status != Active || view.Cost == nil ||
		view.Cost.Cmp(big.NewInt(126)) < 0 || view.Cost.Cmp(big.NewInt(127)) > 0 {
		t.Fatalf("active view %+v cost %v err=%v", view, view.Cost, err)
	}

	finished, err := env.orders.Finish(ctx, started.OrderID)
	if err != nil || finished.Status != Completed || finished.Cost == nil ||
		finished.Cost.Cmp(big.NewInt(126)) < 0 || finished.FinishTime == nil {
		t.Fatalf("finish %+v err=%v", finished, err)
	}
	replay, err := env.orders.Finish(ctx, started.OrderID)
	if err != nil || replay.Cost == nil || replay.Cost.Cmp(finished.Cost) != 0 ||
		replay.FinishTime == nil || !replay.FinishTime.Equal(*finished.FinishTime) {
		t.Fatalf("replay %+v err=%v", replay, err)
	}

	var settle struct {
		OperationID string   `json:"operation_id"`
		TotalAmount *big.Int `json:"total_amount"`
	}
	var settles int
	err = env.store.QueryRow(ctx, `
		SELECT parameters, (SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE')
		FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE'`, started.OrderID).Scan(&settle, &settles)
	if err != nil || settles != 1 || settle.TotalAmount == nil ||
		settle.TotalAmount.Cmp(finished.Cost) != 0 || settle.OperationID != started.OrderID.String() {
		t.Fatalf("SETTLE %+v count=%d err=%v", settle, settles, err)
	}

	short := env.start(t, env.offer(t, "s-short", "zone-1").ID)
	env.activate(t, short.OrderID, 2*time.Second)
	cancelled, err := env.orders.Finish(ctx, short.OrderID)
	if err != nil || cancelled.Status != Cancelled || cancelled.Cost == nil || cancelled.Cost.Sign() != 0 {
		t.Fatalf("short trip %+v err=%v", cancelled, err)
	}
}

// archiveOrder finishes a trip and moves it to history after all of its tasks are done.
func (e *orderEnv) archiveOrder(t *testing.T, ctx context.Context, orderID uuid.UUID) {
	t.Helper()
	e.activate(t, orderID, time.Minute)
	if _, err := e.orders.Finish(ctx, orderID); err != nil {
		t.Fatal(err)
	}
	tests.ExecSQL(t, e.store, `DELETE FROM tasks WHERE order_id = $1`, orderID)
	// Set up history directly so this test isolates order replay from maintenance scheduling.
	tests.ExecSQL(t, e.store, `CREATE TABLE test_orders_history PARTITION OF orders_history DEFAULT`)
	tag, err := e.store.Exec(ctx, `
		WITH removed_order AS (
			DELETE FROM orders WHERE order_id = $1 RETURNING *
		), removed_offer AS (
			DELETE FROM offers f USING removed_order o WHERE f.offer_id = o.offer_id
			RETURNING f.user_profile_fallback_used
		)
		INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate,
			unlock_fee, deposit, status, created_at, started_at, finish_time, closed_at, delete_after,
			cost, user_profile_fallback_used, payload)
		SELECT o.order_id, o.offer_id, o.user_id, o.scooter_id, o.zone_id, o.minute_rate, o.unlock_fee,
			o.deposit, o.status, o.created_at, o.started_at, o.finish_time, o.closed_at, o.delete_after,
			o.cost, f.user_profile_fallback_used, o.payload FROM removed_order o CROSS JOIN removed_offer f`, orderID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("archive fixture for order %s: %d rows, %v", orderID, tag.RowsAffected(), err)
	}
}

func TestStartReplayAfterArchive(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offered := env.offer(t, "s", "zone-1")
	started := env.start(t, offered.ID)
	env.archiveOrder(t, ctx, started.OrderID)

	env.orders.scooters = scootersFunc(func(context.Context, string) (source.Scooter, error) {
		t.Fatal("a replay must not call Source")
		return source.Scooter{}, source.ErrUnavailable
	})
	replay, err := env.orders.Start(ctx, offered.ID)
	if err != nil || !replay.Replayed || replay.OrderID != started.OrderID || replay.Status != Completed {
		t.Fatalf("replay after archive %+v err=%v", replay, err)
	}
	if _, err := env.orders.Start(ctx, uuid.New()); !errors.Is(err, offer.ErrNotFound) {
		t.Fatalf("unknown offer: %v", err)
	}
}

func TestStartReplayWhenSourceFailsDuringArchive(t *testing.T) {
	env := newOrderEnv(t)
	offered := env.offer(t, "s", "zone-1")

	var created StartResult
	service := *env.orders
	service.scooters = scootersFunc(func(ctx context.Context, _ string) (source.Scooter, error) {
		// A second request can finish and archive its order while the first is waiting for Source.
		created = env.start(t, offered.ID)
		env.archiveOrder(t, ctx, created.OrderID)
		return source.Scooter{}, source.ErrUnavailable
	})
	replay, err := service.Start(context.Background(), offered.ID)
	if err != nil || !replay.Replayed || replay.OrderID != created.OrderID || replay.Status != Completed {
		t.Fatalf("replay when Source fails after archiving %+v err=%v", replay, err)
	}
}

func TestStartRollsBackWhenTaskInsertFails(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offer := env.offer(t, "s", "zone-1")
	tests.ExecSQL(t, env.store, `ALTER TABLE tasks ADD CONSTRAINT test_reject_start CHECK (task_type <> 'START')`)
	_, err := env.orders.Start(ctx, offer.ID)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.ConstraintName != "test_reject_start" {
		t.Fatalf("expected the START insert to fail, got %v", err)
	}
	env.requireAvailableOffer(t, ctx, offer.ID, "offer after rollback")
	if _, err := env.orders.repo.findOrderByOffer(ctx, offer.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("order survived rollback: %v", err)
	}
	tests.ExecSQL(t, env.store, `ALTER TABLE tasks DROP CONSTRAINT test_reject_start`)
	if result := env.start(t, offer.ID); result.Replayed {
		t.Fatal("retry after rollback did not create an order")
	}
}

func (e *orderEnv) requireAvailableOffer(t *testing.T, ctx context.Context, id uuid.UUID, description string) {
	t.Helper()
	var status string
	err := e.store.QueryRow(ctx, "SELECT status FROM offers WHERE offer_id = $1", id).Scan(&status)
	if err != nil || status != "AVAILABLE" {
		t.Fatalf("%s: status=%q err=%v", description, status, err)
	}
}
