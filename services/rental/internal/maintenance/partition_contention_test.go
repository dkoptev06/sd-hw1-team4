package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/testdb"
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
			store := testdb.New(t)
			m := newMaintenance(t, store)
			m.cfg.WeeksAhead, m.cfg.BatchSize = test.weeksAhead, 1
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			now, err := store.Now(ctx)
			if err != nil {
				t.Fatal(err)
			}
			week := storage.WeekStart(now)
			old := week.AddDate(0, 0, -7)
			ready := week.AddDate(0, 0, 7)
			missing := week.AddDate(0, 0, 7*test.missingWeekOffset)
			for _, w := range []time.Time{old, week, ready} {
				if err := store.CreatePartition(ctx, w); err != nil {
					t.Fatal(err)
				}
			}
			oldID := insertArchivedOrder(t, store, old.Add(time.Hour))
			control := insertArchivedOrder(t, store, ready.Add(time.Hour))
			deferred, _ := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "30 days"})
			free, offer := insertClosedOrder(t, store, closedOrder{status: domain.OrderCompleted, deleteAfter: "30 days"})
			// The oldest candidate fills the whole batch and needs a missing week.
			testdb.Exec(t, store, `UPDATE orders SET closed_at = closed_at - interval '1 hour',
				delete_after = $2 WHERE order_id = $1`, deferred, missing.Add(time.Hour))
			testdb.Exec(t, store, `UPDATE orders SET delete_after = $2 WHERE order_id = $1`, free, ready.Add(time.Hour))

			oldName := storage.PartitionName(old)
			if err := store.SchedulePartitionDrop(ctx, oldName, old); err != nil {
				t.Fatal(err)
			}
			reader, err := store.Pool().Begin(ctx)
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
				if err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_inherits
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
			if err := m.RunCycle(attempt); err != nil {
				t.Fatalf("archive cycle while DETACH waited: %v", err)
			}
			if _, archived, _, err := store.GetOrder(attempt, control); err != nil || !archived {
				t.Fatalf("existing history became unreadable: archived=%t, %v", archived, err)
			}
			if _, archived, _, err := store.GetOrder(attempt, free); err != nil || !archived {
				t.Fatalf("ready week stalled behind missing week: archived=%t, %v", archived, err)
			}
			if got, err := store.FindOrderByOffer(attempt, offer); err != nil || got.ID != free {
				t.Fatalf("order replay during DETACH: %+v, %v", got, err)
			}
			if _, archived, _, err := store.GetOrder(attempt, deferred); err != nil || archived {
				t.Fatalf("order awaiting partition: archived=%t, %v", archived, err)
			}

			if err := reader.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			<-cleaned
			if cleanupErr != nil {
				t.Fatal(cleanupErr)
			}
			if err := m.RunCycle(ctx); err != nil {
				t.Fatal(err)
			}
			if _, archived, _, err := store.GetOrder(ctx, deferred); err != nil || !archived {
				t.Fatalf("order for missing week after release: archived=%t, %v", archived, err)
			}
			if n := count(t, store, `SELECT count(*) FROM pg_inherits
				WHERE inhparent = 'orders_history'::regclass AND inhrelid = to_regclass($1)`, storage.PartitionName(missing)); n != 1 {
				t.Fatal("required week was not attached after DETACH completed")
			}
		})
	}
}
