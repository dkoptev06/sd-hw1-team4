package maintenance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/testdb"
)

func newMaintenance(t *testing.T, store *storage.Store) *Maintenance {
	t.Helper()
	cfg := settings.Maintenance{
		Interval: time.Minute, Lease: time.Minute, BatchSize: 2, OfferGrace: time.Minute,
		WeeksAhead: 3, StatsInterval: time.Minute,
	}
	return New(store, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

type closedOrder struct {
	status       domain.OrderStatus
	deleteAfter  string // SQL interval from now
	fallbackUsed bool
	task         domain.TaskStatus // "" for no task
}

// insertClosedOrder creates a consumed offer and a closed order, optionally with a SETTLE task.
func insertClosedOrder(t *testing.T, store *storage.Store, o closedOrder) (orderID, offerID uuid.UUID) {
	t.Helper()
	orderID, offerID = uuid.New(), uuid.New()
	testdb.Exec(t, store, `
		INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee,
			deposit, user_profile_fallback_used, status, created_at, expires_at)
		VALUES ($1, $1::uuid::text, 'user', 'scooter', 'zone', 50, 10, 50, 300, $2, 'CONSUMED', clock_timestamp(), clock_timestamp())`,
		offerID, o.fallbackUsed)
	testdb.Exec(t, store, `
		INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit, status,
			created_at, started_at, finish_time, closed_at, delete_after, cost)
		VALUES ($1, $2, 'user', 'scooter', 'zone', 10, 50, 300, $3, clock_timestamp(), clock_timestamp(),
			clock_timestamp(), clock_timestamp(), clock_timestamp() + $4::interval, 70)`,
		orderID, offerID, o.status, o.deleteAfter)
	if o.task != "" {
		finished := any(nil)
		if o.task == domain.TaskCompleted {
			finished = time.Now()
		}
		testdb.Exec(t, store, `
			INSERT INTO tasks (task_id, order_id, task_type, status, parameters, created_at, next_run_at, finished_at)
			VALUES (gen_random_uuid(), $1, 'SETTLE', $2, '{}', clock_timestamp(), clock_timestamp(), $3)`,
			orderID, o.task, finished)
	}
	return orderID, offerID
}

func count(t *testing.T, store *storage.Store, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := store.Pool().QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestArchive(t *testing.T) {
	store := testdb.New(t)
	ctx := context.Background()
	m := newMaintenance(t, store)

	archived, archivedOffer := insertClosedOrder(t, store, closedOrder{
		status: domain.OrderCompleted, deleteAfter: "365 days", fallbackUsed: true, task: domain.TaskCompleted})
	testdb.Exec(t, store, `UPDATE orders SET payload = set_byte(payload, 0, 42) WHERE order_id = $1`, archived)
	shortRetention, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCancelled, deleteAfter: "30 days"})
	expired, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderStartFailed, deleteAfter: "-1 day"})
	inDLQ, _ := insertClosedOrder(t, store, closedOrder{
		status: domain.OrderCompleted, deleteAfter: "-1 day", task: domain.TaskDeadLetter})
	pending, _ := insertClosedOrder(t, store, closedOrder{
		status: domain.OrderCompleted, deleteAfter: "365 days", task: domain.TaskPending})
	testdb.Exec(t, store, `
		INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate, unlock_fee,
			deposit, user_profile_fallback_used, status, created_at, expires_at)
		VALUES (gen_random_uuid(), 'old', 'u', 's', 'z', 50, 1, 1, 1, false, 'AVAILABLE',
			clock_timestamp() - interval '1 hour', clock_timestamp() - interval '1 hour'),
		       (gen_random_uuid(), 'fresh', 'u', 's', 'z', 50, 1, 1, 1, false, 'AVAILABLE',
			clock_timestamp(), clock_timestamp())`)

	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}

	order, isArchived, _, err := store.GetOrder(ctx, archived)
	if err != nil || !isArchived || order.Status != domain.OrderCompleted || order.Cost == nil || order.Cost.String() != "70" {
		t.Fatalf("archived order %+v archived=%t err=%v", order, isArchived, err)
	}
	if ref, err := store.FindOrderByOffer(ctx, archivedOffer); err != nil || ref.ID != archived {
		t.Fatalf("lookup by offer after archiving: %+v %v", ref, err)
	}
	if n := count(t, store, `SELECT count(*) FROM orders_history WHERE order_id = $1 AND user_profile_fallback_used`, archived); n != 1 {
		t.Fatal("user_profile_fallback_used not copied")
	}
	if n := count(t, store, `SELECT get_byte(payload, 0) FROM orders_history WHERE order_id = $1`, archived); n != 42 {
		t.Fatalf("payload was not copied: first byte is %d", n)
	}
	if _, isArchived, _, err := store.GetOrder(ctx, shortRetention); err != nil || !isArchived {
		t.Fatalf("30-day order: archived=%t err=%v", isArchived, err)
	}
	if _, _, _, err := store.GetOrder(ctx, expired); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired order must be deleted without archiving: %v", err)
	}
	for _, id := range []uuid.UUID{inDLQ, pending} {
		if _, isArchived, _, err := store.GetOrder(ctx, id); err != nil || isArchived {
			t.Fatalf("order with an unfinished task left orders: archived=%t err=%v", isArchived, err)
		}
	}
	if n := count(t, store, `SELECT count(*) FROM tasks WHERE status = 'COMPLETED'`); n != 0 {
		t.Fatalf("%d completed tasks left", n)
	}
	if n := count(t, store, `SELECT count(*) FROM offers WHERE status = 'CONSUMED'`); n != 2 {
		t.Fatalf("offers of archived orders must be deleted, %d consumed left", n)
	}
	if n := count(t, store, `SELECT count(*) FROM offers WHERE status = 'AVAILABLE'`); n != 1 {
		t.Fatalf("expired available offer not purged or fresh one deleted: %d left", n)
	}
}

func insertArchivedOrder(t *testing.T, store *storage.Store, deleteAfter time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	testdb.Exec(t, store, `
		INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
			status, created_at, closed_at, delete_after, cost, user_profile_fallback_used)
		VALUES ($1, gen_random_uuid(), 'u', 's', 'z', 1, 1, 1, 'COMPLETED', $2, $2, $2, 0, false)`, id, deleteAfter)
	return id
}

func TestPartitions(t *testing.T) {
	store := testdb.New(t)
	ctx := context.Background()
	m := newMaintenance(t, store)

	expiredWeek := storage.WeekStart(time.Now().AddDate(0, 0, -14))
	if err := store.CreatePartition(ctx, expiredWeek); err != nil {
		t.Fatal(err)
	}
	insertArchivedOrder(t, store, expiredWeek.Add(time.Hour))
	// Identical names and schemas are insufficient evidence for a DROP.
	testdb.Exec(t, store, `CREATE TABLE orders_history_pending_import (id int)`)
	testdb.Exec(t, store, `CREATE TABLE orders_history_p20200106 (LIKE orders_history)`)
	// Exercise a crash after DETACH completed and before DROP.
	detached := expiredWeek.AddDate(0, 0, -7)
	name := storage.PartitionName(detached)
	if err := store.CreatePartition(ctx, detached); err != nil {
		t.Fatal(err)
	}
	if err := store.SchedulePartitionDrop(ctx, name, detached); err != nil {
		t.Fatal(err)
	}
	if err := store.DetachPartition(ctx, name); err != nil {
		t.Fatal(err)
	}
	// A replacement created after a tracked table was lost must survive.
	replaced := expiredWeek.AddDate(0, 0, -14)
	replacement := storage.PartitionName(replaced)
	if err := store.CreatePartition(ctx, replaced); err != nil {
		t.Fatal(err)
	}
	insertArchivedOrder(t, store, replaced)
	if err := store.SchedulePartitionDrop(ctx, replacement, replaced); err != nil {
		t.Fatal(err)
	}
	if err := store.DetachPartition(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	testdb.Exec(t, store, "DROP TABLE "+replacement)
	testdb.Exec(t, store, "CREATE TABLE "+replacement+" (id int)")

	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	partitions, err := store.ListPartitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current := storage.WeekStart(time.Now())
	want := map[string]bool{}
	for i := range 3 {
		want[storage.PartitionName(current.AddDate(0, 0, 7*i))] = true
	}
	if len(partitions) != len(want) {
		t.Fatalf("partitions %+v, want %v", partitions, want)
	}
	for _, p := range partitions {
		if !want[p.Name] {
			t.Fatalf("unexpected partition %s", p.Name)
		}
	}
	if n := count(t, store, `SELECT num_nonnulls(to_regclass($1),to_regclass($2))`, storage.PartitionName(expiredWeek), name); n != 0 {
		t.Fatal("tracked expired/detached partitions still present")
	}
	if n := count(t, store, `SELECT num_nonnulls(to_regclass($1),to_regclass('orders_history_p20200106'),to_regclass('orders_history_pending_import'))`, replacement); n != 3 {
		t.Fatal("unrelated tables were dropped")
	}
	if n := count(t, store, "SELECT count(*) FROM history_partition_cleanup"); n != 0 {
		t.Fatal("orphaned cleanup intents")
	}
	if n := count(t, store, "SELECT count(*) FROM order_history_locator"); n != 0 {
		t.Fatal("orphaned locators")
	}

}

// An unrelated leaf lock must not affect reads, even after repeated execution.
func TestArchivedReadsPruneLockedPartitions(t *testing.T) {
	store := testdb.New(t)
	ctx := t.Context()
	first := storage.WeekStart(time.Now()).AddDate(0, 0, 7)
	for i := range 53 {
		if err := store.CreatePartition(ctx, first.AddDate(0, 0, i*7)); err != nil {
			t.Fatal(err)
		}
	}
	id := insertArchivedOrder(t, store, first)
	order, err := store.GetArchivedOrder(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	locker, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	name := pgx.Identifier{storage.PartitionName(first.AddDate(0, 0, 7))}.Sanitize()
	if _, err := locker.Exec(ctx, "LOCK TABLE ONLY "+name+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		got, archived, _, err := store.GetOrder(readCtx, id)
		cancel()
		if err != nil || !archived || got.ID != id {
			t.Fatalf("archived read: archived=%t order=%v err=%v", archived, got.ID, err)
		}
		readCtx, cancel = context.WithTimeout(ctx, time.Second)
		ref, err := store.FindOrderByOffer(readCtx, order.OfferID)
		cancel()
		if err != nil || ref.ID != id {
			t.Fatalf("archived replay: order=%v err=%v", ref.ID, err)
		}
	}
}

func TestPartitionLocatorCleanupResumesAcrossCycles(t *testing.T) {
	store := testdb.New(t)
	ctx := t.Context()
	m := newMaintenance(t, store)
	m.cfg.BatchSize = 1
	week := storage.WeekStart(time.Now()).AddDate(0, 0, -14)
	if err := store.CreatePartition(ctx, week); err != nil {
		t.Fatal(err)
	}
	var old uuid.UUID
	for range 10 {
		old = insertArchivedOrder(t, store, week)
	}
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetArchivedOrder(ctx, old); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("detached history remains readable: %v", err)
	}
	if left := count(t, store, "SELECT count(*) FROM order_history_locator"); left != 2 {
		t.Fatalf("expected two locators left for the next cycle, got %d", left)
	}
	if n := count(t, store, "SELECT count(*) FROM history_partition_cleanup"); n != 1 {
		t.Fatal("detached partition missing from cleanup journal")
	}
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if left := count(t, store, "SELECT count(*) FROM order_history_locator"); left != 0 {
		t.Fatalf("%d locators survived cleanup", left)
	}
	if n := count(t, store, "SELECT count(*) FROM history_partition_cleanup"); n != 0 {
		t.Fatal("completed cleanup still in journal")
	}
	if n := count(t, store, "SELECT num_nonnulls(to_regclass($1))", storage.PartitionName(week)); n != 0 {
		t.Fatal("detached table survived cleanup")
	}
}

func TestPartitionOnDemand(t *testing.T) {
	store := testdb.New(t)
	ctx := context.Background()
	m := newMaintenance(t, store)
	// Longer than WeeksAhead: the partition is created when the order is archived.
	id, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "400 days"})
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, isArchived, _, err := store.GetOrder(ctx, id); err != nil || !isArchived {
		t.Fatalf("archived=%t err=%v", isArchived, err)
	}
}

func TestPartitionNames(t *testing.T) {
	week := storage.WeekStart(time.Date(2026, 11, 26, 15, 0, 0, 0, time.UTC)) // Thursday
	if week != time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("week start %s", week)
	}
	parsed, ok := storage.ParsePartitionName(storage.PartitionName(week))
	if !ok || !parsed.Equal(week) {
		t.Fatalf("parsed %s %t", parsed, ok)
	}
	if _, ok := storage.ParsePartitionName("orders_history_p20261124"); ok {
		t.Fatal("accepted a partition that does not start on Monday")
	}
}

func TestLeaseIsExclusive(t *testing.T) {
	store := testdb.New(t)
	ctx := context.Background()
	first, second := newMaintenance(t, store), newMaintenance(t, store)
	if held, err := first.renewLease(ctx, &store.Queries); err != nil || !held {
		t.Fatalf("first: %t %v", held, err)
	}
	if held, err := second.renewLease(ctx, &store.Queries); err != nil || held {
		t.Fatalf("second took a live lease: %t %v", held, err)
	}
	if held, err := first.renewLease(ctx, &store.Queries); err != nil || !held {
		t.Fatalf("holder could not renew: %t %v", held, err)
	}
	first.release()
	if held, err := second.renewLease(ctx, &store.Queries); err != nil || !held {
		t.Fatalf("second after release: %t %v", held, err)
	}
	if err := second.exportStats(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionUsesLastDeadlineAndDatabaseClock(t *testing.T) {
	store := testdb.New(t)
	ctx := t.Context()
	actual, err := store.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	week := storage.WeekStart(actual).AddDate(0, 0, -7)
	deadline := week.Add(2 * time.Hour)

	// Make PostgreSQL sessions used by maintenance read a clock in the previous
	// week, while the application's real clock is already past its upper bound.
	testdb.Exec(t, store, `CREATE TABLE maintenance_test_clock (test_now timestamptz NOT NULL)`)
	testdb.Exec(t, store, `INSERT INTO maintenance_test_clock VALUES ($1)`, week.Add(time.Hour))
	testdb.Exec(t, store, `
		CREATE FUNCTION clock_timestamp() RETURNS timestamptz
		LANGUAGE SQL VOLATILE RETURN (SELECT test_now FROM maintenance_test_clock)`)
	cfg := store.Pool().Config()
	cfg.ConnConfig.RuntimeParams["search_path"] += ",pg_catalog"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	shifted := storage.New(pool)
	if now, err := shifted.Now(ctx); err != nil || !now.Equal(week.Add(time.Hour)) {
		t.Fatalf("unexpected database clock: %v, %v", now, err)
	}

	if err := store.CreatePartition(ctx, week); err != nil {
		t.Fatal(err)
	}
	insertArchivedOrder(t, store, week)
	live := insertArchivedOrder(t, store, deadline)
	m := newMaintenance(t, shifted)
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.GetOrder(ctx, live); err != nil {
		t.Fatalf("partition lost a live order while only the host clock passed the week: %v", err)
	}

	testdb.Exec(t, store, `UPDATE maintenance_test_clock SET test_now = $1`, deadline)
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	name := storage.PartitionName(week)
	if n := count(t, store, `SELECT count(to_regclass($1))`, name); n != 0 {
		t.Fatal("partition still exists after the last deadline in the database, before the end of the week")
	}

	// A later order can use the same week after the expired partition was dropped.
	testdb.Exec(t, store, "CREATE TABLE "+name+" (marker int)")
	testdb.Exec(t, store, "INSERT INTO "+name+" VALUES (42)")
	newOrder, _ := insertClosedOrder(t, shifted, closedOrder{status: domain.OrderCompleted, deleteAfter: "1 hour"})
	if err := m.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, archived, _, err := store.GetOrder(ctx, newOrder); err != nil || !archived {
		t.Fatalf("expected order in a recreated partition after recovering the detached table: archived=%t, %v", archived, err)
	}
	if count(t, store, "SELECT marker FROM "+name) != 42 {
		t.Fatal("legacy untracked table modified")
	}
	replacement, err := store.ListPartitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, p := range replacement {
		if w, ok := storage.ParsePartitionName(p.Name); ok && w.Equal(week) && p.Name != name {
			found = true
		}
	}
	if !found {
		t.Fatal("replacement for the occupied partition name not found")
	}

}

func TestArchiveContinuesWhileCleanupSessionHoldsDetach(t *testing.T) {
	store := testdb.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	now, err := store.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	week := storage.WeekStart(now)
	previous := week.AddDate(0, 0, -7)
	if err := store.CreatePartition(ctx, previous); err != nil {
		t.Fatal(err)
	}
	insertArchivedOrder(t, store, previous.Add(time.Hour))

	first, second := newMaintenance(t, store), newMaintenance(t, store)
	first.cfg.Lease = 100 * time.Millisecond
	for i := range first.cfg.WeeksAhead {
		if err := store.CreatePartition(ctx, week.AddDate(0, 0, 7*i)); err != nil {
			t.Fatal(err)
		}
	}
	locker, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	var oldRows int
	if err := locker.QueryRow(ctx, `
		SELECT count(*) FROM orders_history WHERE delete_after >= $1 AND delete_after < $2`, previous, week).
		Scan(&oldRows); err != nil || oldRows != 1 {
		t.Fatalf("failed to hold a reader on the expired partition: rows=%d, %v", oldRows, err)
	}

	finished := make(chan error, 1)
	go func() { finished <- first.RunCycle(ctx) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	partition := storage.PartitionName(previous)
	for {
		var canAttemptTakeover bool
		err := store.Pool().QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_inherits
				WHERE inhrelid = to_regclass($1) AND inhdetachpending)
				AND EXISTS (SELECT 1 FROM maintenance_leases
					WHERE name = 'maintenance' AND holder = $2 AND lease_until <= clock_timestamp())`,
			partition, first.holder).Scan(&canAttemptTakeover)
		if err != nil {
			t.Fatal(err)
		}
		if canAttemptTakeover {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("DETACH finished before its reader released the lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}

	next, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "1 day"})
	attempt, stop := context.WithTimeout(ctx, time.Second)
	err = second.RunCycle(attempt)
	stop()
	if err != nil {
		t.Fatalf("concurrent archive cycle: %v", err)
	}
	if _, archived, _, err := store.GetOrder(ctx, next); err != nil || !archived {
		t.Fatalf("new orders failed to archive while the old reader held DETACH: archived=%t, %v", archived, err)
	}
	if n := count(t, store, "SELECT count(*) FROM history_partition_cleanup"); n != 1 {
		t.Fatalf("another replica finished cleanup during the reader: %d intents", n)
	}

	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := second.RunCycle(ctx); err != nil {
		t.Fatal(err)
	}
	if _, archived, _, err := store.GetOrder(ctx, next); err != nil || !archived {
		t.Fatalf("second replica did not resume archiving: archived=%t, %v", archived, err)
	}
}

// This state can exist when upgrading from a release that did not journal a
// concurrent DETACH. Preserve new orders for that week and archive other weeks
// while an old reader prevents FINALIZE, including in a one-connection pool.
func TestRecoveryAdoptsPendingDetachBeforeArchiving(t *testing.T) {
	store := testdb.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	now, err := store.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	week := storage.WeekStart(now)
	for _, w := range []time.Time{week, week.AddDate(0, 0, 7)} {
		if err := store.CreatePartition(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	old := insertArchivedOrder(t, store, week)
	locker, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	if _, err := locker.Exec(ctx, `SELECT order_id FROM orders_history WHERE order_id=$1`, old); err != nil {
		t.Fatal(err)
	}

	cfg := store.Pool().Config()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	single := storage.New(pool)
	name := storage.PartitionName(week)
	var originalOID uint32
	if err := store.Pool().QueryRow(ctx, "SELECT to_regclass($1)::oid", name).Scan(&originalOID); err != nil {
		t.Fatal(err)
	}
	const pendingDetach = `SELECT count(*) FROM pg_inherits
        WHERE inhparent='orders_history'::regclass AND inhrelid=$1 AND inhdetachpending`
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var lockErr *pgconn.PgError
	// Under concurrent DDL a timeout can also precede the first commit. Keep
	// the reader open until PostgreSQL confirms the intended crash state.
	for {
		err = single.WithLockTimeout(ctx, 100*time.Millisecond, func() error { return single.DetachPartition(ctx, name) })
		if !errors.As(err, &lockErr) || lockErr.Code != "55P03" {
			t.Fatalf("expected interrupted DETACH: %v", err)
		}
		if count(t, store, pendingDetach, originalOID) == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if count(t, store, "SELECT count(*) FROM history_partition_cleanup") != 0 {
		t.Fatal("legacy pending DETACH already has a cleanup intent")
	}

	same, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "1 day"})
	other, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "7 days"})
	testdb.Exec(t, store, `UPDATE orders SET delete_after=$2 WHERE order_id=$1`, same, week.AddDate(0, 0, 7).Add(-time.Microsecond))
	m := newMaintenance(t, single)
	m.cfg.WeeksAhead = 2
	err = m.RunCycle(ctx)
	if !errors.As(err, &lockErr) || lockErr.Code != "55P03" {
		t.Fatalf("expected bounded FINALIZE: %v", err)
	}
	if _, archived, _, err := store.GetOrder(ctx, same); err != nil || archived {
		t.Fatalf("order for retiring week: archived=%t, %v", archived, err)
	}
	if _, archived, _, err := store.GetOrder(ctx, other); err != nil || !archived {
		t.Fatalf("unrelated week: archived=%t, %v", archived, err)
	}
	if err := locker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Other sessions can still hold an old snapshot after our reader exits.
	// The real worker resumes on subsequent cycles with the same cleanup intent.
	for {
		err = m.RunCycle(ctx)
		if err != nil {
			if !errors.As(err, &lockErr) || lockErr.Code != "55P03" || count(t, store, pendingDetach, originalOID) != 1 {
				t.Fatalf("unexpected recovery error: %v", err)
			}
		}
		_, archived, _, readErr := store.GetOrder(ctx, same)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if archived && count(t, store, "SELECT count(*) FROM pg_class WHERE oid=$1", originalOID) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pending DETACH failed to recover: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if _, _, _, err := store.GetOrder(ctx, old); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired order survived recovery: %v", err)
	}
	if count(t, store, "SELECT count(*) FROM history_partition_cleanup") != 0 {
		t.Fatal("cleanup intent survived recovery")
	}
	if count(t, store, "SELECT count(*) FROM orders_history WHERE order_id=$1", same) != 1 {
		t.Fatal("new order must appear exactly once in history")
	}
}
