package maintenance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

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
	if err != nil || !isArchived || order.Status != domain.OrderCompleted || *order.Cost != 70 {
		t.Fatalf("archived order %+v archived=%t err=%v", order, isArchived, err)
	}
	if ref, err := store.FindArchivedOrderByOffer(ctx, archivedOffer); err != nil || ref.ID != archived {
		t.Fatalf("lookup by offer after archiving: %+v %v", ref, err)
	}
	if n := count(t, store, `SELECT count(*) FROM orders_history WHERE order_id = $1 AND user_profile_fallback_used`, archived); n != 1 {
		t.Fatal("user_profile_fallback_used not copied")
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

func TestPartitions(t *testing.T) {
	store := testdb.New(t)
	ctx := context.Background()
	m := newMaintenance(t, store)

	expiredWeek := storage.WeekStart(time.Now().AddDate(0, 0, -14))
	if err := store.CreatePartition(ctx, expiredWeek); err != nil {
		t.Fatal(err)
	}
	testdb.Exec(t, store, `
		INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
			status, created_at, closed_at, delete_after, cost, user_profile_fallback_used)
		VALUES (gen_random_uuid(), gen_random_uuid(), 'u', 's', 'z', 1, 1, 1, 'COMPLETED', $1, $1, $1, 0, false)`,
		expiredWeek.Add(time.Hour))
	// A table left detached by an interrupted cleanup.
	testdb.Exec(t, store, `CREATE TABLE orders_history_p20200106 (LIKE orders_history)`)

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
	if n := count(t, store, `SELECT count(*) FROM pg_class WHERE relname IN ($1, 'orders_history_p20200106')`,
		storage.PartitionName(expiredWeek)); n != 0 {
		t.Fatal("expired or detached partition tables were not dropped")
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
	if held, err := first.renewLease(ctx); err != nil || !held {
		t.Fatalf("first: %t %v", held, err)
	}
	if held, err := second.renewLease(ctx); err != nil || held {
		t.Fatalf("second took a live lease: %t %v", held, err)
	}
	if held, err := first.renewLease(ctx); err != nil || !held {
		t.Fatalf("holder could not renew: %t %v", held, err)
	}
	first.release()
	if held, err := second.renewLease(ctx); err != nil || !held {
		t.Fatalf("second after release: %t %v", held, err)
	}
	if err := second.exportStats(ctx); err != nil {
		t.Fatal(err)
	}
}
