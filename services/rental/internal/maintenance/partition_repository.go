package maintenance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// partitionRow is an orders_history partition.
type partitionRow struct {
	Name          string
	DetachPending bool
	Scheduled     bool
}

// listPartitions returns the attached partitions of orders_history.
func (q repository) listPartitions(ctx context.Context) ([]partitionRow, error) {
	rows, err := q.db.Query(ctx, `
		SELECT c.relname, i.inhdetachpending, EXISTS (
			SELECT 1 FROM history_partition_cleanup j WHERE j.partition_oid = c.oid)
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'orders_history'::regclass ORDER BY c.relname`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[partitionRow])
}

// schedulePartitionDrop records the identity while PostgreSQL still knows the parent.
func (q repository) schedulePartitionDrop(ctx context.Context, name string, week time.Time) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO history_partition_cleanup (partition_oid, partition_name, week_start)
		SELECT c.oid, c.relname, $2 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'orders_history'::regclass AND c.oid = to_regclass($1)
		ON CONFLICT DO NOTHING`, name, week)
	return err
}

type partitionDropRow struct {
	OID      uint32
	Name     string
	Attached bool
	Pending  bool
}

func (q repository) pendingPartitionDrops(ctx context.Context) ([]partitionDropRow, error) {
	rows, err := q.db.Query(ctx, `
		SELECT j.partition_oid, j.partition_name, i.inhrelid IS NOT NULL, coalesce(i.inhdetachpending, false)
		FROM history_partition_cleanup j LEFT JOIN pg_inherits i
			ON i.inhparent = 'orders_history'::regclass AND i.inhrelid = j.partition_oid
			AND i.inhrelid = to_regclass(quote_ident(j.partition_name))
		ORDER BY i.inhdetachpending DESC NULLS LAST, j.week_start`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[partitionDropRow])
}

// Creating partitions ahead must also skip a detached table still awaiting DROP.
func (q repository) partitionDropScheduled(ctx context.Context, week time.Time) (bool, error) {
	var scheduled bool
	err := q.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM history_partition_cleanup WHERE week_start = $1)`, week).Scan(&scheduled)
	return scheduled, err
}

// createPartition creates the partition of the given week if it does not exist.
func (q repository) createPartition(ctx context.Context, week time.Time) error {
	week = weekStart(week)
	name := partitionName(week)
	var occupied bool
	if err := q.db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&occupied); err != nil {
		return err
	}
	if occupied {
		partitions, err := q.listPartitions(ctx)
		if err != nil {
			return err
		}
		for _, p := range partitions {
			if start, ok := parsePartitionName(p.Name); ok && week.Equal(start) {
				return nil
			}
		}
		// An untracked table (including one detached before the journal was
		// introduced) keeps its name and data. Use a fresh name for this week.
		name += "_" + uuid.NewString()
	}
	_, err := q.db.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s PARTITION OF orders_history FOR VALUES FROM ('%s') TO ('%s')`,
		pgx.Identifier{name}.Sanitize(),
		week.Format(time.RFC3339), week.AddDate(0, 0, 7).Format(time.RFC3339)))
	return err
}

// tryCreatePartition bounds the parent DDL lock wait. A reader or a pending
// DETACH may hold it for an entire cycle; orders for ready weeks can still move.
func (q repository) tryCreatePartition(ctx context.Context, week time.Time) (bool, error) {
	err := pgx.BeginFunc(ctx, q.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='50ms'"); err != nil {
			return err
		}
		return (repository{db: tx}).createPartition(ctx, week)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
		return false, nil
	}
	return err == nil, err
}

// latestPartitionExpiry reads the last row's deadline using the retention index.
// An empty partition returns pgx.ErrNoRows.
func (q repository) latestPartitionExpiry(ctx context.Context, name string) (time.Time, error) {
	var latest time.Time
	err := q.db.QueryRow(ctx, `SELECT delete_after FROM `+pgx.Identifier{name}.Sanitize()+
		` ORDER BY delete_after DESC LIMIT 1`).Scan(&latest)
	return latest, err
}

// detachPartition detaches a partition without blocking readers. It must run outside a transaction.
func (q repository) detachPartition(ctx context.Context, name string) error {
	_, err := q.db.Exec(ctx, `ALTER TABLE orders_history DETACH PARTITION `+pgx.Identifier{name}.Sanitize()+` CONCURRENTLY`)
	return err
}

// finalizeDetach completes a DETACH ... CONCURRENTLY interrupted by a failure.
func (q repository) finalizeDetach(ctx context.Context, name string) error {
	_, err := q.db.Exec(ctx, `ALTER TABLE orders_history DETACH PARTITION `+pgx.Identifier{name}.Sanitize()+` FINALIZE`)
	return err
}

// finishPartitionDrop deletes a bounded locator batch. The detached table and
// journal entry remain available for retries until the last batch commits.
func (q repository) finishPartitionDrop(ctx context.Context, p partitionDropRow, limit int) (dropped, more bool, err error) {
	name := pgx.Identifier{p.Name}.Sanitize()
	err = pgx.BeginFunc(ctx, q.db, func(tx pgx.Tx) error {
		const matches = `SELECT EXISTS (SELECT 1 FROM pg_class
			WHERE oid = $1 AND oid = to_regclass($2) AND NOT relispartition AND relkind = 'r')`
		var same, missing bool
		if err := tx.QueryRow(ctx, matches, p.OID, name).Scan(&same); err != nil {
			return err
		}
		if !same {
			// An externally removed table can leave locators behind. A renamed
			// or replacement relation is never dropped on the journal's authority.
			if err := tx.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=$1)`, p.OID).Scan(&missing); err != nil {
				return err
			}
		}
		if same || missing {
			var cursor *uuid.UUID
			if err := tx.QueryRow(ctx, `
				SELECT locator_cursor FROM history_partition_cleanup WHERE partition_oid=$1`, p.OID).Scan(&cursor); err != nil {
				return err
			}
			const byPartition = `SELECT order_id FROM order_history_locator WHERE partition_oid = $1::oid::regclass`
			batch := byPartition
			args := []any{p.OID, limit}
			if cursor != nil {
				batch += ` AND order_id > $3`
				args = append(args, *cursor)
			}
			// Advance past deleted index entries even if vacuum has not removed them yet.
			var deleted int
			var last *uuid.UUID
			err := tx.QueryRow(ctx, `
				WITH batch AS MATERIALIZED (`+batch+` ORDER BY order_id LIMIT $2),
				deleted AS (
					DELETE FROM order_history_locator l USING batch b WHERE l.order_id=b.order_id RETURNING l.order_id
				)
				SELECT (SELECT count(*) FROM deleted), (SELECT order_id FROM batch ORDER BY order_id DESC LIMIT 1)`,
				args...).Scan(&deleted, &last)
			if err != nil {
				return err
			}
			if deleted == limit {
				more = true
				_, err := tx.Exec(ctx, `UPDATE history_partition_cleanup SET locator_cursor=$2 WHERE partition_oid=$1`, p.OID, last)
				return err
			}
		}
		if same {
			if _, err := tx.Exec(ctx, "LOCK TABLE ONLY "+name+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, matches, p.OID, name).Scan(&same); err != nil {
				return err
			}
			if same {
				if _, err := tx.Exec(ctx, "DROP TABLE "+name); err != nil {
					return err
				}
				dropped = true
			}
		}
		_, err := tx.Exec(ctx, `DELETE FROM history_partition_cleanup WHERE partition_oid=$1`, p.OID)
		return err
	})
	return dropped, more, err
}

// withLockTimeout lets a one-connection pool serve archive work between DDL retries.
func (q repository) withLockTimeout(ctx context.Context, timeout time.Duration, run func() error) error {
	if _, err := q.db.Exec(ctx, `SELECT set_config('lock_timeout', $1, false)`, fmt.Sprintf("%dms", timeout.Milliseconds())); err != nil {
		return err
	}
	err := run()
	resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resetErr := q.db.Exec(resetCtx, "RESET lock_timeout")
	return errors.Join(err, resetErr)
}
