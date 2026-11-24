package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// WithMaintenanceLock guards a maintenance cycle on the same PostgreSQL session
// that runs its statements. An expired lease cannot grant a second cycle while
// the first session is still running, even if its client process has stopped.
func (s *Store) WithMaintenanceLock(ctx context.Context, name string, run func(*Queries) error) (bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	// The lease table OID scopes the lock to the resolved schema in this database.
	const key = "hashtextextended($1, 'maintenance_leases'::regclass::oid::bigint)"
	var lockChecked, held bool
	defer func() {
		if lockChecked && !held {
			conn.Release()
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if held && conn.QueryRow(cleanup, "SELECT pg_advisory_unlock("+key+")", name).Scan(&unlocked) == nil && unlocked {
			conn.Release()
			return
		}
		// If we cannot confirm the lock was released, discard the session.
		_ = conn.Hijack().Close(cleanup)
	}()
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock("+key+")", name).Scan(&held); err != nil {
		return false, err
	}
	lockChecked = true
	if !held {
		return false, nil
	}
	return true, run(&Queries{db: conn})
}

// AcquireLease takes or extends a named lease for holder. It returns false while
// another holder has a live lease.
func (q *Queries) AcquireLease(ctx context.Context, name string, holder uuid.UUID, duration time.Duration) (bool, error) {
	var got uuid.UUID
	err := q.db.QueryRow(ctx, `
		INSERT INTO maintenance_leases (name, holder, lease_until)
		VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, lease_until = EXCLUDED.lease_until
		WHERE maintenance_leases.lease_until <= clock_timestamp() OR maintenance_leases.holder = EXCLUDED.holder
		RETURNING holder`, name, holder, duration.Seconds()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ReleaseLease gives the lease up so another process can take it immediately.
func (q *Queries) ReleaseLease(ctx context.Context, name string, holder uuid.UUID) error {
	_, err := q.db.Exec(ctx, `DELETE FROM maintenance_leases WHERE name = $1 AND holder = $2`, name, holder)
	return err
}

// PurgeCompletedTasks deletes up to limit COMPLETED tasks, oldest first.
func (q *Queries) PurgeCompletedTasks(ctx context.Context, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		DELETE FROM tasks WHERE task_id IN (
			SELECT task_id FROM tasks WHERE status = 'COMPLETED'
			ORDER BY finished_at, task_id LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit)
	return tag.RowsAffected(), err
}

// archiveCandidates selects the next archive batch. ArchiveCandidateWeeks and
// ArchiveOrders share it so that pre-created partitions match the archived rows.
const archiveCandidates = `
	SELECT o.order_id, o.delete_after FROM orders o
	WHERE o.closed_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.order_id = o.order_id)
		AND (o.delete_after <= clock_timestamp() OR NOT EXISTS (
			SELECT 1 FROM history_partition_cleanup j
			WHERE j.week_start = date_trunc('week', o.delete_after, 'UTC')))`

const oldestArchiveCandidates = ` ORDER BY o.closed_at, o.order_id LIMIT $1`

// ArchiveCandidateWeeks returns the history weeks the next archive batch will write to.
func (q *Queries) ArchiveCandidateWeeks(ctx context.Context, limit int) ([]time.Time, error) {
	rows, err := q.db.Query(ctx, `
		SELECT DISTINCT c.delete_after FROM (`+archiveCandidates+oldestArchiveCandidates+`) c
		WHERE c.delete_after > clock_timestamp()`, limit)
	if err != nil {
		return nil, err
	}
	deleteAfter, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	if err != nil {
		return nil, err
	}
	seen := map[time.Time]bool{}
	var weeks []time.Time
	for _, t := range deleteAfter {
		if week := WeekStart(t); !seen[week] {
			seen[week] = true
			weeks = append(weeks, week)
		}
	}
	return weeks, nil
}

// ArchiveOrders moves up to limit closed orders without tasks to orders_history
// and deletes them with their offers in one statement. Orders whose delete_after
// already passed are deleted without archiving. The history partitions must exist.
func (q *Queries) ArchiveOrders(ctx context.Context, limit int, readyWeeks []time.Time) (removed, archived int64, err error) {
	err = q.db.QueryRow(ctx, `
		WITH batch AS (`+archiveCandidates+`
            AND (o.delete_after <= clock_timestamp() OR date_trunc('week', o.delete_after, 'UTC') = ANY($2::timestamptz[]))
            `+oldestArchiveCandidates+`
			FOR UPDATE OF o SKIP LOCKED
		), removed_orders AS (
			DELETE FROM orders o USING batch WHERE o.order_id = batch.order_id
			RETURNING o.*
		), removed_offers AS (
			DELETE FROM offers f USING removed_orders r WHERE f.offer_id = r.offer_id
			RETURNING f.offer_id, f.user_profile_fallback_used
		), archived AS (
			INSERT INTO orders_history (`+orderColumns+`, user_profile_fallback_used, payload)
			SELECT r.order_id, r.offer_id, r.user_id, r.scooter_id, r.zone_id, r.minute_rate, r.unlock_fee,
				r.deposit, r.status, r.created_at, r.started_at, r.finish_time, r.closed_at, r.delete_after,
				r.cost, f.user_profile_fallback_used, r.payload
			FROM removed_orders r JOIN removed_offers f ON f.offer_id = r.offer_id
			WHERE r.delete_after > clock_timestamp()
			RETURNING 1
		)
		SELECT (SELECT count(*) FROM removed_orders), (SELECT count(*) FROM archived)`, limit, readyWeeks).
		Scan(&removed, &archived)
	return removed, archived, err
}

// IsMissingPartition reports an insert into orders_history without a matching partition.
func IsMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && strings.Contains(pgErr.Message, "no partition")
}

// PurgeExpiredOffers deletes AVAILABLE offers without an order that expired more than grace ago.
func (q *Queries) PurgeExpiredOffers(ctx context.Context, grace time.Duration, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		DELETE FROM offers WHERE offer_id IN (
			SELECT f.offer_id FROM offers f
			WHERE f.status = 'AVAILABLE' AND f.expires_at < clock_timestamp() - make_interval(secs => $2)
				AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.offer_id = f.offer_id)
			ORDER BY f.expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit, grace.Seconds())
	return tag.RowsAffected(), err
}

// History partitions cover one week each, from Monday 00:00 UTC, and are named orders_history_pYYYYMMDD.
const partitionPrefix = "orders_history_p"

// WeekStart returns the Monday 00:00 UTC of the week containing t.
func WeekStart(t time.Time) time.Time {
	day := t.UTC().Truncate(24 * time.Hour)
	return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
}

func PartitionName(week time.Time) string {
	return partitionPrefix + week.Format("20060102")
}

// ParsePartitionName returns the week of a partition created by CreatePartition.
func ParsePartitionName(name string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(name, partitionPrefix)
	if !ok {
		return time.Time{}, false
	}
	date, suffix, hasSuffix := strings.Cut(rest, "_")
	if hasSuffix {
		if _, err := uuid.Parse(suffix); err != nil || len(suffix) != 36 {
			return time.Time{}, false
		}
	}
	week, err := time.Parse("20060102", date)
	return week, err == nil && week.Equal(WeekStart(week))
}

// Partition is an orders_history partition.
type Partition struct {
	Name          string
	DetachPending bool
	Scheduled     bool
}

// ListPartitions returns the attached partitions of orders_history.
func (q *Queries) ListPartitions(ctx context.Context) ([]Partition, error) {
	rows, err := q.db.Query(ctx, `
		SELECT c.relname, i.inhdetachpending, EXISTS (
			SELECT 1 FROM history_partition_cleanup j WHERE j.partition_oid = c.oid)
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'orders_history'::regclass ORDER BY c.relname`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Partition])
}

// SchedulePartitionDrop records the identity while PostgreSQL still knows the parent.
func (q *Queries) SchedulePartitionDrop(ctx context.Context, name string, week time.Time) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO history_partition_cleanup (partition_oid, partition_name, week_start)
		SELECT c.oid, c.relname, $2 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'orders_history'::regclass AND c.oid = to_regclass($1)
		ON CONFLICT DO NOTHING`, name, week)
	return err
}

type PartitionDrop struct {
	OID      uint32
	Name     string
	Attached bool
	Pending  bool
}

func (q *Queries) PendingPartitionDrops(ctx context.Context) ([]PartitionDrop, error) {
	rows, err := q.db.Query(ctx, `
		SELECT j.partition_oid, j.partition_name, i.inhrelid IS NOT NULL, coalesce(i.inhdetachpending, false)
		FROM history_partition_cleanup j LEFT JOIN pg_inherits i
			ON i.inhparent = 'orders_history'::regclass AND i.inhrelid = j.partition_oid
			AND i.inhrelid = to_regclass(quote_ident(j.partition_name))
		ORDER BY i.inhdetachpending DESC NULLS LAST, j.week_start`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[PartitionDrop])
}

// Creating partitions ahead must also skip a detached table still awaiting DROP.
func (q *Queries) PartitionDropScheduled(ctx context.Context, week time.Time) (bool, error) {
	var scheduled bool
	err := q.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM history_partition_cleanup WHERE week_start = $1)`, week).Scan(&scheduled)
	return scheduled, err
}

// CreatePartition creates the partition of the given week if it does not exist.
func (q *Queries) CreatePartition(ctx context.Context, week time.Time) error {
	week = WeekStart(week)
	name := PartitionName(week)
	var occupied bool
	if err := q.db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&occupied); err != nil {
		return err
	}
	if occupied {
		partitions, err := q.ListPartitions(ctx)
		if err != nil {
			return err
		}
		for _, p := range partitions {
			if start, ok := ParsePartitionName(p.Name); ok && week.Equal(start) {
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

// TryCreatePartition bounds the parent DDL lock wait. A reader or a pending
// DETACH may hold it for an entire cycle; orders for ready weeks can still move.
func (q *Queries) TryCreatePartition(ctx context.Context, week time.Time) (bool, error) {
	err := pgx.BeginFunc(ctx, q.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='50ms'"); err != nil {
			return err
		}
		return (&Queries{db: tx}).CreatePartition(ctx, week)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
		return false, nil
	}
	return err == nil, err
}

// LatestPartitionExpiry reads the last row's deadline using the retention index.
// An empty partition returns ErrNotFound.
func (q *Queries) LatestPartitionExpiry(ctx context.Context, name string) (time.Time, error) {
	var latest time.Time
	err := q.db.QueryRow(ctx, `SELECT delete_after FROM `+pgx.Identifier{name}.Sanitize()+
		` ORDER BY delete_after DESC LIMIT 1`).Scan(&latest)
	return latest, notFound(err)
}

// DetachPartition detaches a partition without blocking readers. It must run outside a transaction.
func (q *Queries) DetachPartition(ctx context.Context, name string) error {
	_, err := q.db.Exec(ctx, `ALTER TABLE orders_history DETACH PARTITION `+pgx.Identifier{name}.Sanitize()+` CONCURRENTLY`)
	return err
}

// FinalizeDetach completes a DETACH ... CONCURRENTLY interrupted by a failure.
func (q *Queries) FinalizeDetach(ctx context.Context, name string) error {
	_, err := q.db.Exec(ctx, `ALTER TABLE orders_history DETACH PARTITION `+pgx.Identifier{name}.Sanitize()+` FINALIZE`)
	return err
}

// FinishPartitionDrop deletes a bounded locator batch. The detached table and
// journal entry remain available for retries until the last batch commits.
func (q *Queries) FinishPartitionDrop(ctx context.Context, p PartitionDrop, limit int) (dropped, more bool, err error) {
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

// WithLockTimeout lets a one-connection pool serve archive work between DDL retries.
func (q *Queries) WithLockTimeout(ctx context.Context, timeout time.Duration, run func() error) error {
	if _, err := q.db.Exec(ctx, `SELECT set_config('lock_timeout', $1, false)`, fmt.Sprintf("%dms", timeout.Milliseconds())); err != nil {
		return err
	}
	err := run()
	resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resetErr := q.db.Exec(resetCtx, "RESET lock_timeout")
	return errors.Join(err, resetErr)
}

// Stats is a snapshot of the database state for monitoring.
type Stats struct {
	Tasks                []TaskStat
	OldestTaskAge        map[string]float64
	Orders               map[string]int64
	OldestClosedOrderAge float64
	DatabaseBytes        int64
}

type TaskStat struct {
	Kind   string
	Status string
	Count  int64
}

func (q *Queries) Stats(ctx context.Context) (Stats, error) {
	s := Stats{OldestTaskAge: map[string]float64{}, Orders: map[string]int64{}}
	rows, err := q.db.Query(ctx, `SELECT task_type, status, count(*) FROM tasks GROUP BY 1, 2`)
	if err != nil {
		return s, err
	}
	if s.Tasks, err = pgx.CollectRows(rows, pgx.RowToStructByPos[TaskStat]); err != nil {
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
			pg_database_size(current_database())
		FROM orders WHERE closed_at IS NOT NULL`).Scan(&s.OldestClosedOrderAge, &s.DatabaseBytes)
	return s, err
}
