package maintenance

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// databaseStats is a snapshot of the database state for monitoring.
type databaseStats struct {
	Tasks                []taskStatRow
	OldestTaskAge        map[string]float64
	Orders               map[string]int64
	OldestClosedOrderAge float64
	OldestReadyOrderAge  float64
	DatabaseBytes        int64
}

type taskStatRow struct {
	Kind   string
	Status string
	Count  int64
}

func (q repository) stats(ctx context.Context) (databaseStats, error) {
	s := databaseStats{OldestTaskAge: map[string]float64{}, Orders: map[string]int64{}}
	rows, err := q.db.Query(ctx, `SELECT task_type, status, count(*) FROM tasks GROUP BY 1, 2`)
	if err != nil {
		return s, err
	}
	if s.Tasks, err = pgx.CollectRows(rows, pgx.RowToStructByPos[taskStatRow]); err != nil {
		return s, err
	}
	rows, err = q.db.Query(ctx, `
		SELECT status, extract(epoch FROM clock_timestamp() - min(COALESCE(finished_at, created_at)))::float8
		FROM tasks GROUP BY status`)
	if err != nil {
		return s, err
	}
	var status string
	var age float64
	if _, err = pgx.ForEachRow(rows, []any{&status, &age}, func() error {
		s.OldestTaskAge[status] = age
		return nil
	}); err != nil {
		return s, err
	}
	rows, err = q.db.Query(ctx, `SELECT status, count(*) FROM orders GROUP BY status`)
	if err != nil {
		return s, err
	}
	var count int64
	if _, err = pgx.ForEachRow(rows, []any{&status, &count}, func() error {
		s.Orders[status] = count
		return nil
	}); err != nil {
		return s, err
	}
	err = q.db.QueryRow(ctx, `
		SELECT COALESCE(extract(epoch FROM clock_timestamp() - min(closed_at))::float8, 0),
			COALESCE((
				SELECT extract(epoch FROM clock_timestamp() - o.closed_at)::float8
				FROM orders o
				WHERE o.closed_at IS NOT NULL
					AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.order_id = o.order_id)
				ORDER BY o.closed_at
				LIMIT 1
			), 0),
			pg_database_size(current_database())
		FROM orders WHERE closed_at IS NOT NULL`).Scan(&s.OldestClosedOrderAge, &s.OldestReadyOrderAge, &s.DatabaseBytes)
	return s, err
}
