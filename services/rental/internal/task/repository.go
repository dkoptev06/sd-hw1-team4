package task

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
)

const taskColumns = `task_id, order_id, task_type, status, parameters, attempt_count, claim_id, lease_until,
	next_run_at, deadline_at`

type taskRow struct {
	id, orderID  uuid.UUID
	kind, status string
	params       []byte
	attempts     int
	claimID      *uuid.UUID
	leaseUntil   *time.Time
	nextRunAt    time.Time
	deadline     *time.Time
}

func scanTask(row pgx.Row, extra ...any) (entry, error) {
	var r taskRow
	dest := []any{&r.id, &r.orderID, &r.kind, &r.status, &r.params, &r.attempts, &r.claimID,
		&r.leaseUntil, &r.nextRunAt, &r.deadline}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return entry{}, err
	}
	t, err := restore(r.id, r.orderID, Kind(r.kind), Status(r.status), r.params, schedule{
		attempts: r.attempts, claimID: r.claimID, leaseUntil: r.leaseUntil,
		nextRunAt: r.nextRunAt, deadline: r.deadline,
	})
	if err != nil {
		return entry{}, fmt.Errorf("restore task %s: %w", r.id, err)
	}
	return t, nil
}

type repository struct{ db postgres.DBTX }

// MaxAttempts is the attempt limit per task kind.
type MaxAttempts map[Kind]int

func (m MaxAttempts) args() (start, settle, void int) {
	return m[Start], m[Settle], m[Void]
}

const maxAttemptsSQL = `CASE task_type WHEN 'START' THEN $1::int WHEN 'SETTLE' THEN $2::int ELSE $3::int END`

const readyForAttemptSQL = `status = 'PENDING' AND next_run_at <= clock_timestamp()
	AND (lease_until IS NULL OR lease_until <= clock_timestamp())
	AND attempt_count < ` + maxAttemptsSQL + `
	AND (deadline_at IS NULL OR deadline_at > clock_timestamp())`

func (q repository) insert(ctx context.Context, item entry) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO tasks (task_id, order_id, task_type, parameters, created_at, next_run_at, deadline_at)
		VALUES ($1, $2, $3, $4, $5, $5, $6)`, item.id, item.orderID, item.kind, item.params, item.nextRunAt, item.deadline)
	return err
}

// claim claims up to limit due tasks with a new claim_id and lease, incrementing
// attempt_count. START leases never extend past the deadline. Tasks without attempts
// left or with a passed deadline are left to the sweeps.
func (q repository) claim(ctx context.Context, limit int, lease time.Duration, maxAttempts MaxAttempts) ([]Claim, error) {
	start, settle, void := maxAttempts.args()
	rows, err := q.db.Query(ctx, `
		WITH picked AS (
			SELECT task_id FROM tasks
			WHERE `+readyForAttemptSQL+`
			ORDER BY next_run_at, task_id
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks t
		SET claim_id = gen_random_uuid(),
			lease_until = LEAST(clock_timestamp() + make_interval(secs => $5), COALESCE(t.deadline_at, 'infinity')),
			attempt_count = t.attempt_count + 1
		FROM picked WHERE t.task_id = picked.task_id
		RETURNING t.task_id, t.order_id, t.task_type, t.status, t.parameters, t.attempt_count, t.claim_id,
			t.lease_until, t.next_run_at, t.deadline_at, extract(epoch FROM clock_timestamp() - t.next_run_at)::float8`,
		start, settle, void, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var claimed []Claim
	for rows.Next() {
		var lagSeconds float64
		task, err := scanTask(rows, &lagSeconds)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, Claim{value: task, Lag: time.Duration(lagSeconds * float64(time.Second))})
	}
	return claimed, rows.Err()
}

// readyStats uses the same eligibility as claim without locking or claiming rows.
func (q repository) readyStats(ctx context.Context, maxAttempts MaxAttempts) (map[Kind]ReadyStats, error) {
	start, settle, void := maxAttempts.args()
	rows, err := q.db.Query(ctx, `
		SELECT task_type, count(*),
			extract(epoch FROM clock_timestamp() - min(GREATEST(next_run_at, COALESCE(lease_until, next_run_at))))::float8
		FROM tasks
		WHERE `+readyForAttemptSQL+`
		GROUP BY task_type`, start, settle, void)
	if err != nil {
		return nil, err
	}
	result := make(map[Kind]ReadyStats)
	var kind Kind
	var stats ReadyStats
	_, err = pgx.ForEachRow(rows, []any{&kind, &stats.Count, &stats.OldestWaitSeconds}, func() error {
		result[kind] = stats
		return nil
	})
	return result, err
}

// lock locks a task by ID.
func (q repository) lock(ctx context.Context, id uuid.UUID) (entry, error) {
	return scanTask(q.db.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE task_id = $1 FOR UPDATE`, id))
}

// tryLock is lock that does not wait: a task locked by another
// transaction is reported as pgx.ErrNoRows.
func (q repository) tryLock(ctx context.Context, id uuid.UUID) (entry, error) {
	return scanTask(q.db.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE task_id = $1 FOR UPDATE SKIP LOCKED`, id))
}

// lockStart locks the START task of an order. Lock the order first.
func (q repository) lockStart(ctx context.Context, orderID uuid.UUID) (entry, error) {
	return scanTask(q.db.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE order_id = $1 AND task_type = 'START' FOR UPDATE`, orderID))
}

// updateHold corrects the HOLD amount under the first START claim's lock.
func (q repository) updateHold(ctx context.Context, id uuid.UUID, params holdRecord) error {
	_, err := q.db.Exec(ctx, `UPDATE tasks SET parameters = $2 WHERE task_id = $1`, id, params)
	return err
}

// complete finalizes a task, releases its claim and returns the persisted last error.
// An empty lastError keeps the previous one.
func (q repository) complete(ctx context.Context, id uuid.UUID, finishedAt time.Time, result string, reason *Reason, lastError string) (string, error) {
	var persistedError string
	err := q.db.QueryRow(ctx, `
		UPDATE tasks SET status = 'COMPLETED', finished_at = $2, result = $3, completion_reason = $4,
			last_error = COALESCE(NULLIF($5, ''), last_error), claim_id = NULL, lease_until = NULL
		WHERE task_id = $1
		RETURNING COALESCE(last_error, '')`, id, finishedAt, result, reason, lastError).Scan(&persistedError)
	return persistedError, err
}

// reschedule records a transient failure and releases the claim until nextRunAt.
func (q repository) reschedule(ctx context.Context, id uuid.UUID, nextRunAt time.Time, lastError string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE tasks SET next_run_at = $2, last_error = $3, claim_id = NULL, lease_until = NULL
		WHERE task_id = $1`, id, nextRunAt, lastError)
	return err
}

// deadLetter stops automatic attempts and returns the persisted last error.
func (q repository) deadLetter(ctx context.Context, id uuid.UUID, lastError string) (string, error) {
	var persistedError string
	err := q.db.QueryRow(ctx, `
		UPDATE tasks SET status = 'DEAD_LETTER', last_error = COALESCE(NULLIF($2, ''), last_error),
			claim_id = NULL, lease_until = NULL
		WHERE task_id = $1
		RETURNING COALESCE(last_error, '')`, id, lastError).Scan(&persistedError)
	return persistedError, err
}

// SweepTask identifies a sweep candidate and its position in the ordered scan.
// A nil Position starts a new scan before the first candidate.
type SweepTask struct {
	ID       uuid.UUID
	OrderID  uuid.UUID
	Kind     Kind
	Position *time.Time
}

// findExpiredStarts returns the next page of START tasks ordered by deadline and ID.
func (q repository) findExpiredStarts(ctx context.Context, limit int, cutoff time.Time, after SweepTask) ([]SweepTask, error) {
	rows, err := q.db.Query(ctx, `
		SELECT task_id, order_id, task_type, deadline_at FROM tasks
		WHERE task_type = 'START' AND status = 'PENDING' AND deadline_at <= $4
			AND (deadline_at, task_id) > (COALESCE($2::timestamptz, '-infinity'), $3::uuid)
		ORDER BY deadline_at, task_id LIMIT $1`, limit, after.Position, after.ID, cutoff)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[SweepTask])
}

// findExhausted returns the next page of tasks with no attempts left and no live lease.
func (q repository) findExhausted(ctx context.Context, limit int, maxAttempts MaxAttempts, cutoff time.Time, after SweepTask) ([]SweepTask, error) {
	start, settle, void := maxAttempts.args()
	rows, err := q.db.Query(ctx, `
		SELECT task_id, order_id, task_type, next_run_at FROM tasks
		WHERE status = 'PENDING' AND attempt_count >= `+maxAttemptsSQL+` AND created_at <= $7
			AND (lease_until IS NULL OR lease_until <= $7)
			AND (next_run_at, task_id) > (COALESCE($5::timestamptz, '-infinity'), $6::uuid)
		ORDER BY next_run_at, task_id LIMIT $4`, start, settle, void, limit, after.Position, after.ID, cutoff)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[SweepTask])
}

// DeadLetterEntry is a task waiting for manual recovery.
type DeadLetterEntry struct {
	ID           uuid.UUID
	OrderID      uuid.UUID
	Kind         Kind
	AttemptCount int
	LastError    *string
}

func (q repository) listDeadLetters(ctx context.Context) ([]DeadLetterEntry, error) {
	rows, err := q.db.Query(ctx, `
		SELECT task_id, order_id, task_type, attempt_count, last_error FROM tasks
		WHERE status = 'DEAD_LETTER' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[DeadLetterEntry])
}

// requeueDeadLetters returns dead-lettered tasks (one, or all when id is nil) to PENDING
// with a reset attempt count. Parameters and operation_id stay the same.
func (q repository) requeueDeadLetters(ctx context.Context, id *uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE tasks SET status = 'PENDING', attempt_count = 0, claim_id = NULL, lease_until = NULL,
			next_run_at = clock_timestamp()
		WHERE status = 'DEAD_LETTER' AND ($1::uuid IS NULL OR task_id = $1)`, id)
	return tag.RowsAffected(), err
}
