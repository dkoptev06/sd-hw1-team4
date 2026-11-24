package maintenance

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

type repository struct{ db postgres.DBTX }

const orderColumns = `order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit,
	status, created_at, started_at, finish_time, closed_at, delete_after, cost`

// withSessionLock guards a maintenance cycle on the same PostgreSQL session
// that runs its statements. An expired lease cannot grant a second cycle while
// the first session is still running, even if its client process has stopped.
func withSessionLock(ctx context.Context, pool *pgxpool.Pool, name string, run func(repository) error) (bool, error) {
	conn, err := pool.Acquire(ctx)
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
	return true, run(repository{db: conn})
}

// acquireLease takes or extends a named lease for holder. It returns false while
// another holder has a live lease.
func (q repository) acquireLease(ctx context.Context, name string, holder uuid.UUID, duration time.Duration) (bool, error) {
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

// releaseLease gives the lease up so another process can take it immediately.
func (q repository) releaseLease(ctx context.Context, name string, holder uuid.UUID) error {
	_, err := q.db.Exec(ctx, `DELETE FROM maintenance_leases WHERE name = $1 AND holder = $2`, name, holder)
	return err
}

// purgeCompletedTasks deletes up to limit COMPLETED tasks, oldest first.
func (q repository) purgeCompletedTasks(ctx context.Context, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		DELETE FROM tasks WHERE task_id IN (
			SELECT task_id FROM tasks WHERE status = 'COMPLETED'
			ORDER BY finished_at, task_id LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit)
	return tag.RowsAffected(), err
}

// archiveCandidates selects the next archive batch. archiveCandidateWeeks and
// archiveOrders share it so that pre-created partitions match the archived rows.
const archiveCandidates = `
	SELECT o.order_id, o.delete_after FROM orders o
	WHERE o.closed_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.order_id = o.order_id)
		AND (o.delete_after <= clock_timestamp() OR NOT EXISTS (
			SELECT 1 FROM history_partition_cleanup j
			WHERE j.week_start = date_trunc('week', o.delete_after, 'UTC')))`

const oldestArchiveCandidates = ` ORDER BY o.closed_at, o.order_id LIMIT $1`

// archiveCandidateWeeks returns the history weeks the next archive batch will write to.
func (q repository) archiveCandidateWeeks(ctx context.Context, limit int) ([]time.Time, error) {
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
		if week := weekStart(t); !seen[week] {
			seen[week] = true
			weeks = append(weeks, week)
		}
	}
	return weeks, nil
}

// archiveOrders moves up to limit closed orders without tasks to orders_history
// and deletes them with their offers in one statement. Orders whose delete_after
// already passed are deleted without archiving. The history partitions must exist.
func (q repository) archiveOrders(ctx context.Context, limit int, readyWeeks []time.Time) (removed, archived int64, err error) {
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

// isMissingPartition reports an insert into orders_history without a matching partition.
func isMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && strings.Contains(pgErr.Message, "no partition")
}

// purgeExpiredOffers deletes AVAILABLE offers without an order that expired more than grace ago.
func (q repository) purgeExpiredOffers(ctx context.Context, grace time.Duration, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		DELETE FROM offers WHERE offer_id IN (
			SELECT f.offer_id FROM offers f
			WHERE f.status = 'AVAILABLE' AND f.expires_at < clock_timestamp() - make_interval(secs => $2)
				AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.offer_id = f.offer_id)
			ORDER BY f.expires_at LIMIT $1 FOR UPDATE SKIP LOCKED)`, limit, grace.Seconds())
	return tag.RowsAffected(), err
}
