package maintenance

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func TestArchiveReadyAgeExcludesOrdersWithTasks(t *testing.T) {
	pool := tests.NewDB(t)
	repo := repository{db: pool}
	for _, status := range []task.Status{task.Pending, task.Completed, task.DeadLetter} {
		insertClosedOrder(t, pool, closedOrder{status: order.Completed, deleteAfter: "30 days", task: status})
	}
	tests.ExecSQL(t, pool, `UPDATE orders SET closed_at = clock_timestamp() - interval '1 hour'`)

	stats, err := repo.stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.OldestClosedOrderAge < 3600 || stats.OldestReadyOrderAge != 0 {
		t.Fatalf("with blocked orders only: oldest closed %.2fs, oldest ready %.2fs", stats.OldestClosedOrderAge, stats.OldestReadyOrderAge)
	}

	readyID, _ := insertClosedOrder(t, pool, closedOrder{status: order.Completed, deleteAfter: "30 days"})
	tests.ExecSQL(t, pool, `UPDATE orders SET closed_at = clock_timestamp() - interval '90 seconds' WHERE order_id = $1`, readyID)
	stats, err = repo.stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stats.OldestReadyOrderAge < 90 || stats.OldestReadyOrderAge >= stats.OldestClosedOrderAge {
		t.Fatalf("with an order ready: oldest closed %.2fs, oldest ready %.2fs", stats.OldestClosedOrderAge, stats.OldestReadyOrderAge)
	}
}

func TestReadyTaskStatsUsesClaimEligibility(t *testing.T) {
	pool := tests.NewDB(t)
	m := newMaintenance(t, pool)
	rows := []struct {
		kind     task.Kind
		status   task.Status
		nextRun  string
		lease    any
		deadline any
		attempts int
	}{
		{task.Settle, task.Pending, "-10 seconds", nil, nil, 0},
		{task.Settle, task.Pending, "-20 minutes", "-35 seconds", nil, 1},
		{task.Settle, task.Pending, "1 hour", nil, nil, 0},
		{task.Settle, task.Pending, "-1 hour", "1 hour", nil, 1},
		{task.Settle, task.Pending, "-1 hour", nil, nil, 20},
		{task.Settle, task.DeadLetter, "-1 hour", nil, nil, 5},
		{task.Start, task.Pending, "-1 hour", nil, "-1 minute", 1},
		{task.Start, task.Pending, "-20 seconds", nil, "1 hour", 1},
	}
	for _, row := range rows {
		id, _ := insertClosedOrder(t, pool, closedOrder{status: order.Completed, deleteAfter: "30 days", task: row.status})
		tests.ExecSQL(t, pool, `
			UPDATE tasks
			SET task_type = $2, next_run_at = clock_timestamp() + $3::interval,
				lease_until = clock_timestamp() + $4::interval,
				claim_id = CASE WHEN $4::interval IS NOT NULL THEN gen_random_uuid() END,
				deadline_at = clock_timestamp() + $5::interval, attempt_count = $6
			WHERE order_id = $1`, id, row.kind, row.nextRun, row.lease, row.deadline, row.attempts)
	}

	ready, err := m.tasks.ReadyStats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if settle := ready[task.Settle]; settle.Count != 2 || settle.OldestWaitSeconds < 35 || settle.OldestWaitSeconds >= 1200 {
		t.Fatalf("SETTLE ready stats must count the expired lease and measure wait since it expired: %+v", settle)
	}
	if start := ready[task.Start]; start.Count != 1 || start.OldestWaitSeconds < 20 || start.OldestWaitSeconds >= 3600 {
		t.Fatalf("START ready stats must omit the expired deadline: %+v", start)
	}
	if void := ready[task.Void]; void.Count != 0 || void.OldestWaitSeconds != 0 {
		t.Fatalf("VOID queue should be empty: %+v", void)
	}

	claims, err := m.tasks.Claim(t.Context(), 20)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[task.Kind]int64)
	for _, claim := range claims {
		counts[claim.Kind()]++
	}
	for _, kind := range []task.Kind{task.Start, task.Settle, task.Void} {
		if counts[kind] != ready[kind].Count {
			t.Errorf("%s: claimed %d tasks, reported %d ready", kind, counts[kind], ready[kind].Count)
		}
	}
}
