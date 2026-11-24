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
	ORDER BY o.closed_at, o.order_id LIMIT $1`

// ArchiveCandidateWeeks returns the history weeks the next archive batch will write to.
func (q *Queries) ArchiveCandidateWeeks(ctx context.Context, limit int) ([]time.Time, error) {
	rows, err := q.db.Query(ctx, `
		SELECT DISTINCT c.delete_after FROM (`+archiveCandidates+`) c
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
func (q *Queries) ArchiveOrders(ctx context.Context, limit int) (removed, archived int64, err error) {
	err = q.db.QueryRow(ctx, `
		WITH batch AS (`+archiveCandidates+`
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
		SELECT (SELECT count(*) FROM removed_orders), (SELECT count(*) FROM archived)`, limit).
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
	week, err := time.Parse("20060102", rest)
	return week, err == nil && week.Equal(WeekStart(week))
}

// Partition is an orders_history partition.
type Partition struct {
	Name          string
	DetachPending bool
}

// ListPartitions returns the attached partitions of orders_history.
func (q *Queries) ListPartitions(ctx context.Context) ([]Partition, error) {
	rows, err := q.db.Query(ctx, `
		SELECT c.relname, i.inhdetachpending FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'orders_history'::regclass ORDER BY c.relname`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Partition])
}

// ListDetachedPartitions returns tables named like history partitions that are no
// longer attached, left behind by an interrupted cleanup.
func (q *Queries) ListDetachedPartitions(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, `
		SELECT c.relname FROM pg_class c
		WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'orders_history'::regclass)
			AND c.relkind = 'r' AND NOT c.relispartition AND starts_with(c.relname, $1)
		ORDER BY c.relname`, partitionPrefix)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// CreatePartition creates the partition of the given week if it does not exist.
func (q *Queries) CreatePartition(ctx context.Context, week time.Time) error {
	week = WeekStart(week)
	_, err := q.db.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF orders_history FOR VALUES FROM ('%s') TO ('%s')`,
		pgx.Identifier{PartitionName(week)}.Sanitize(),
		week.Format(time.RFC3339), week.AddDate(0, 0, 7).Format(time.RFC3339)))
	return err
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

func (q *Queries) DropTable(ctx context.Context, name string) error {
	_, err := q.db.Exec(ctx, `DROP TABLE IF EXISTS `+pgx.Identifier{name}.Sanitize())
	return err
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
