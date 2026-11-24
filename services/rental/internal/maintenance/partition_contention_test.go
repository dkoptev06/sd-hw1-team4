package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func TestMissingPartitionDuringDetach(t *testing.T) {
	for _, test := range []struct {
		name                          string
		weeksAhead, missingWeekOffset int
	}{
		{"missing week in precreation horizon", 3, 2},
		{"older order needs an on-demand week", 2, 12},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := tests.NewDB(t)
			m := newMaintenance(t, store)
			m.cfg.WeeksAhead, m.cfg.BatchSize = test.weeksAhead, 1
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			now, err := postgres.Now(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			week := weekStart(now)
			old := week.AddDate(0, 0, -7)
			ready := week.AddDate(0, 0, 7)
			missing := week.AddDate(0, 0, 7*test.missingWeekOffset)
			for _, w := range []time.Time{old, week, ready} {
				if err := (repository{db: store}).createPartition(ctx, w); err != nil {
					t.Fatal(err)
				}
			}
			oldID := insertArchivedOrder(t, store, old.Add(time.Hour))
			control := insertArchivedOrder(t, store, ready.Add(time.Hour))
			deferred, _ := insertClosedOrder(t, store, closedOrder{status: order.Completed, deleteAfter: "30 days"})
			free, offer := insertClosedOrder(t, store, closedOrder{status: order.Completed, deleteAfter: "30 days"})
			// The oldest candidate fills the whole batch and needs a missing week.
			tests.ExecSQL(t, store, `UPDATE orders SET closed_at = closed_at - interval '1 hour',
				delete_after = $2 WHERE order_id = $1`, deferred, missing.Add(time.Hour))
			tests.ExecSQL(t, store, `UPDATE orders SET delete_after = $2 WHERE order_id = $1`, free, ready.Add(time.Hour))

			oldName := partitionName(old)
			if err := (repository{db: store}).schedulePartitionDrop(ctx, oldName, old); err != nil {
				t.Fatal(err)
			}
			reader, err := store.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback(context.Background())
			if _, err := reader.Exec(ctx, "SELECT order_id FROM orders_history WHERE order_id = $1", oldID); err != nil {
				t.Fatal(err)
			}

			cleaned := make(chan struct{})
			var cleanupErr error
			go func() {
				defer close(cleaned)
				cleanupErr = m.cleanPartitions(ctx)
			}()
			defer func() {
				_ = reader.Rollback(context.Background())
				<-cleaned
			}()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				var pending bool
				if err := store.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_inherits
					WHERE inhrelid = to_regclass($1) AND inhdetachpending)`, oldName).Scan(&pending); err != nil {
					t.Fatal(err)
				}
				if pending {
					break
				}
				select {
				case <-cleaned:
					t.Fatalf("cleanup exited before DETACH waited on the reader: %v", cleanupErr)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}

			attempt, stop := context.WithTimeout(ctx, 3*time.Second)
			defer stop()
			if err := runMaintenanceCycle(m, attempt); err != nil {
				t.Fatalf("archive cycle while DETACH waited: %v", err)
			}
			if view, err := readOrders(store).Get(attempt, control); err != nil || !view.Archived {
				t.Fatalf("existing history became unreadable: archived=%t, %v", view.Archived, err)
			}
			if view, err := readOrders(store).Get(attempt, free); err != nil || !view.Archived {
				t.Fatalf("ready week stalled behind missing week: archived=%t, %v", view.Archived, err)
			}
			if got, err := readOrders(store).Start(attempt, offer); err != nil || got.OrderID != free {
				t.Fatalf("order replay during DETACH: %+v, %v", got, err)
			}
			if view, err := readOrders(store).Get(attempt, deferred); err != nil || view.Archived {
				t.Fatalf("order awaiting partition: archived=%t, %v", view.Archived, err)
			}

			if err := reader.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			<-cleaned
			if cleanupErr != nil {
				t.Fatal(cleanupErr)
			}
			if err := runMaintenanceCycle(m, ctx); err != nil {
				t.Fatal(err)
			}
			if view, err := readOrders(store).Get(ctx, deferred); err != nil || !view.Archived {
				t.Fatalf("order for missing week after release: archived=%t, %v", view.Archived, err)
			}
			if n := count(t, store, `SELECT count(*) FROM pg_inherits
				WHERE inhparent = 'orders_history'::regclass AND inhrelid = to_regclass($1)`, partitionName(missing)); n != 1 {
				t.Fatal("required week was not attached after DETACH completed")
			}
		})
	}
}
