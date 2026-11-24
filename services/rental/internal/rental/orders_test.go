package rental

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/testdb"
)

// scooterBoard is a mutable fake of the scooters source.
type scooterBoard struct {
	mu       sync.Mutex
	scooters map[string]domain.Scooter
	err      error
}

func (b *scooterBoard) Scooter(_ context.Context, id string) (domain.Scooter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return domain.Scooter{}, b.err
	}
	s, ok := b.scooters[id]
	if !ok {
		return domain.Scooter{}, source.ErrNotFound
	}
	return s, nil
}

func (b *scooterBoard) set(s domain.Scooter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.scooters[s.ID] = s
}

type orderEnv struct {
	store    *storage.Store
	offers   *OfferService
	orders   *OrderService
	scooters *scooterBoard
	deposit  int64
}

func newOrderEnv(t *testing.T) *orderEnv {
	t.Helper()
	store := testdb.New(t)
	retention, err := domain.NewRetention(365, map[string]int{"zone-2": 30})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := dynconfig.Defaults()
	snapshot.Retention = retention
	configs := staticConfigs{snapshot}

	env := &orderEnv{store: store, scooters: &scooterBoard{scooters: map[string]domain.Scooter{}}, deposit: 300}
	tariffs := tariffsFunc(func(_ context.Context, zoneID string) (domain.Tariff, error) {
		return domain.Tariff{ZoneID: zoneID, PricePerMinute: 60, PriceUnlock: 50, DefaultDeposit: env.deposit}, nil
	})
	profiles := profilesFunc(func(_ context.Context, id string) (domain.UserProfile, error) {
		return domain.UserProfile{UserID: id}, nil
	})
	env.offers = NewOfferService(store, env.scooters, tariffs, profiles, configs, 2*time.Minute)
	env.orders = NewOrderService(store, env.scooters, configs, 2*time.Minute, time.Hour, nil)
	return env
}

func (e *orderEnv) offer(t *testing.T, scooterID, zoneID string) domain.Offer {
	t.Helper()
	e.scooters.set(domain.Scooter{ID: scooterID, ZoneID: zoneID, Charge: 80})
	offer, _, err := e.offers.Create(context.Background(), CreateOfferRequest{
		ScooterID: scooterID, UserID: "user", IdempotencyID: uuid.NewString(),
	})
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
	testdb.Exec(t, e.store, `
		UPDATE orders SET status = 'ACTIVE', started_at = clock_timestamp() - make_interval(secs => $2)
		WHERE order_id = $1`, orderID, ago.Seconds())
	testdb.Exec(t, e.store, `
		UPDATE tasks SET status = 'COMPLETED', finished_at = clock_timestamp(), result = 'success'
		WHERE order_id = $1 AND task_type = 'START'`, orderID)
}

func TestCreateOfferPersists(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	env.scooters.set(domain.Scooter{ID: "s", ZoneID: "zone-1", Charge: 80})
	req := CreateOfferRequest{ScooterID: "s", UserID: "u", IdempotencyID: "key"}
	first, created, err := env.offers.Create(ctx, req)
	if err != nil || !created || first.Status != domain.OfferAvailable ||
		first.ExpiresAt.Sub(first.CreatedAt) != 2*time.Minute {
		t.Fatalf("create: %+v created=%t err=%v", first, created, err)
	}
	replay, created, err := env.offers.Create(ctx, req)
	if err != nil || created || replay.ID != first.ID || !replay.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("replay: %+v created=%t err=%v", replay, created, err)
	}
	stored, inserted, err := env.store.InsertOffer(ctx, first, time.Minute)
	if err != nil || inserted || stored.ID != uuid.Nil {
		t.Fatalf("duplicate key insert: inserted=%t err=%v", inserted, err)
	}
}

func TestStartCreatesOrderAndTask(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offer := env.offer(t, "s", "zone-1")
	result := env.start(t, offer.ID)
	if result.Replayed || result.Status != domain.OrderStarting {
		t.Fatalf("start: %+v", result)
	}

	var params domain.HoldParams
	var deadline, created time.Time
	err := env.store.Pool().QueryRow(ctx, `
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
	if err != nil || view.Order.Status != domain.OrderStarting || view.Cost != nil || view.Order.Terms != offer.Terms {
		t.Fatalf("view %+v err=%v", view, err)
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
			case errors.Is(errs[i], domain.ErrScooterBusy):
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

	if _, err := env.orders.Start(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown offer: %v", err)
	}

	expired := env.offer(t, "s-expired", "zone-1")
	testdb.Exec(t, env.store, `UPDATE offers SET expires_at = clock_timestamp() WHERE offer_id = $1`, expired.ID)
	if _, err := env.orders.Start(ctx, expired.ID); !errors.Is(err, domain.ErrOfferExpired) {
		t.Fatalf("expired offer: %v", err)
	}

	moved := env.offer(t, "s-moved", "zone-1")
	env.scooters.set(domain.Scooter{ID: "s-moved", ZoneID: "zone-2", Charge: 80})
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, domain.ErrOfferChanged) {
		t.Fatalf("zone changed: %v", err)
	}
	env.scooters.set(domain.Scooter{ID: "s-moved", ZoneID: "zone-1", Charge: 27})
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, domain.ErrOfferChanged) {
		t.Fatalf("charge crossed threshold: %v", err)
	}
	env.scooters.set(domain.Scooter{ID: "s-moved", ZoneID: "zone-1", Charge: 40})
	busyOffer := env.offer(t, "s-moved", "zone-1")

	env.scooters.err = fmt.Errorf("%w: timeout", source.ErrUnavailable)
	if _, err := env.orders.Start(ctx, moved.ID); !errors.Is(err, domain.ErrSourceUnavailable) {
		t.Fatalf("Source down: %v", err)
	}
	if _, err := env.store.FindOrderByOffer(ctx, moved.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed start left an order: %v", err)
	}
	env.scooters.err = nil

	// The charge changed within the same category: the start is allowed.
	env.start(t, moved.ID)
	if _, err := env.orders.Start(ctx, busyOffer.ID); !errors.Is(err, domain.ErrScooterBusy) {
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
	testdb.Exec(t, env.store, `
		UPDATE tasks SET deadline_at = clock_timestamp() - interval '1 second'
		WHERE order_id = $1 AND task_type = 'START'`, previous.OrderID)

	replacement := env.start(t, env.offer(t, "s", "zone-2").ID)
	if replacement.Status != domain.OrderStarting {
		t.Fatalf("replacement %+v", replacement)
	}
	old, err := env.orders.Get(ctx, previous.OrderID)
	if err != nil || old.Order.Status != domain.OrderStartFailed || old.Cost == nil || old.Cost.Sign() != 0 {
		t.Fatalf("previous order %+v err=%v", old, err)
	}
	if days := old.Order.DeleteAfter.Sub(*old.Order.ClosedAt).Hours() / 24; days != 30 {
		t.Fatalf("zone retention %v days", days)
	}
	var taskStatus string
	var voids int
	err = env.store.Pool().QueryRow(ctx, `
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
	testdb.Exec(t, env.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, previous.OrderID)

	tx, err := env.store.Pool().Begin(ctx)
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
	err = env.store.Pool().QueryRow(ctx, `
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
		err := env.store.Pool().QueryRow(ctx, `
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

	if _, err := env.store.Pool().Exec(ctx, `SELECT pg_sleep_until($1)`, expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, domain.ErrOfferExpired) {
		t.Fatalf("start after waiting past the offer expiration: %v", err)
	}
	stored, err := env.store.GetOffer(ctx, next.ID)
	if err != nil || stored.Status != domain.OfferAvailable {
		t.Fatalf("offer changed after rejected start: %+v, %v", stored, err)
	}
	if _, err := env.store.FindOrderByOffer(ctx, next.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("rejected start left an order: %v", err)
	}
	old, err := env.orders.Get(ctx, previous.OrderID)
	if err != nil || old.Order.Status != domain.OrderStarting {
		t.Fatalf("rejected start changed previous order: %+v, %v", old, err)
	}
}

func TestFinish(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	started := env.start(t, env.offer(t, "s", "zone-1").ID)
	if _, err := env.orders.Finish(ctx, started.OrderID); !errors.Is(err, domain.ErrOrderNotActive) {
		t.Fatalf("finish STARTING: %v", err)
	}
	if _, err := env.orders.Finish(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("finish unknown: %v", err)
	}

	env.activate(t, started.OrderID, 61*time.Second)
	view, err := env.orders.Get(ctx, started.OrderID)
	// minute rate floor(60 × 1.25) = 75; cost 50 + floor(61 × 75 / 60) = 126; the read may run a second later.
	if err != nil || view.Order.Status != domain.OrderActive || view.Cost == nil ||
		view.Cost.Cmp(big.NewInt(126)) < 0 || view.Cost.Cmp(big.NewInt(127)) > 0 {
		t.Fatalf("active view %+v cost %v err=%v", view, view.Cost, err)
	}

	finished, err := env.orders.Finish(ctx, started.OrderID)
	if err != nil || finished.Status != domain.OrderCompleted || finished.Cost == nil ||
		finished.Cost.Cmp(big.NewInt(126)) < 0 || finished.FinishTime == nil {
		t.Fatalf("finish %+v err=%v", finished, err)
	}
	replay, err := env.orders.Finish(ctx, started.OrderID)
	if err != nil || replay.Cost == nil || replay.Cost.Cmp(finished.Cost) != 0 ||
		replay.FinishTime == nil || !replay.FinishTime.Equal(*finished.FinishTime) {
		t.Fatalf("replay %+v err=%v", replay, err)
	}

	var settle domain.SettleParams
	var settles int
	err = env.store.Pool().QueryRow(ctx, `
		SELECT parameters, (SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE')
		FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE'`, started.OrderID).Scan(&settle, &settles)
	if err != nil || settles != 1 || settle.TotalAmount == nil ||
		settle.TotalAmount.Cmp(finished.Cost) != 0 || settle.OperationID != started.OrderID.String() {
		t.Fatalf("SETTLE %+v count=%d err=%v", settle, settles, err)
	}

	short := env.start(t, env.offer(t, "s-short", "zone-1").ID)
	env.activate(t, short.OrderID, 2*time.Second)
	cancelled, err := env.orders.Finish(ctx, short.OrderID)
	if err != nil || cancelled.Status != domain.OrderCancelled || cancelled.Cost == nil || cancelled.Cost.Sign() != 0 {
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
	testdb.Exec(t, e.store, `DELETE FROM tasks WHERE order_id = $1`, orderID)
	weeks, err := e.store.ArchiveCandidateWeeks(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, week := range weeks {
		if err := e.store.CreatePartition(ctx, week); err != nil {
			t.Fatal(err)
		}
	}
	if _, archived, err := e.store.ArchiveOrders(ctx, 10, weeks); err != nil || archived != 1 {
		t.Fatalf("archive: %d %v", archived, err)
	}
}

func TestStartReplayAfterArchive(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offered := env.offer(t, "s", "zone-1")
	started := env.start(t, offered.ID)
	env.archiveOrder(t, ctx, started.OrderID)

	env.orders.scooters = scootersFunc(func(context.Context, string) (domain.Scooter, error) {
		t.Fatal("a replay must not call Source")
		return domain.Scooter{}, source.ErrUnavailable
	})
	replay, err := env.orders.Start(ctx, offered.ID)
	if err != nil || !replay.Replayed || replay.OrderID != started.OrderID || replay.Status != domain.OrderCompleted {
		t.Fatalf("replay after archive %+v err=%v", replay, err)
	}
	if _, err := env.orders.Start(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown offer: %v", err)
	}
}

func TestStartReplayWhenSourceFailsDuringArchive(t *testing.T) {
	env := newOrderEnv(t)
	offered := env.offer(t, "s", "zone-1")

	var created StartResult
	service := *env.orders
	service.scooters = scootersFunc(func(ctx context.Context, _ string) (domain.Scooter, error) {
		// A second request can finish and archive its order while the first is waiting for Source.
		created = env.start(t, offered.ID)
		env.archiveOrder(t, ctx, created.OrderID)
		return domain.Scooter{}, source.ErrUnavailable
	})
	replay, err := service.Start(context.Background(), offered.ID)
	if err != nil || !replay.Replayed || replay.OrderID != created.OrderID || replay.Status != domain.OrderCompleted {
		t.Fatalf("replay when Source fails after archiving %+v err=%v", replay, err)
	}
}

func TestStartRollsBackWhenTaskInsertFails(t *testing.T) {
	env := newOrderEnv(t)
	ctx := context.Background()
	offer := env.offer(t, "s", "zone-1")
	testdb.Exec(t, env.store, `ALTER TABLE tasks ADD CONSTRAINT test_reject_start CHECK (task_type <> 'START')`)
	_, err := env.orders.Start(ctx, offer.ID)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.ConstraintName != "test_reject_start" {
		t.Fatalf("expected the START insert to fail, got %v", err)
	}
	stored, err := env.store.GetOffer(ctx, offer.ID)
	if err != nil || stored.Status != domain.OfferAvailable {
		t.Fatalf("offer after rollback: %+v %v", stored, err)
	}
	if _, err := env.store.FindOrderByOffer(ctx, offer.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("order survived rollback: %v", err)
	}
	testdb.Exec(t, env.store, `ALTER TABLE tasks DROP CONSTRAINT test_reject_start`)
	if result := env.start(t, offer.ID); result.Replayed {
		t.Fatal("retry after rollback did not create an order")
	}
}
