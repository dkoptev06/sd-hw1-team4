package storage

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
)

const taskColumns = `task_id, order_id, task_type, status, parameters, attempt_count, claim_id, lease_until,
	next_run_at, deadline_at`

func scanTask(row pgx.Row, extra ...any) (domain.Task, error) {
	var t domain.Task
	var params []byte
	dest := append([]any{&t.ID, &t.OrderID, &t.Kind, &t.Status, &params, &t.AttemptCount, &t.ClaimID,
		&t.LeaseUntil, &t.NextRunAt, &t.Deadline}, extra...)
	if err := row.Scan(dest...); err != nil {
		return domain.Task{}, notFound(err)
	}
	t.Params = params
	t.LeaseUntil, t.Deadline = utc(t.LeaseUntil), utc(t.Deadline)
	t.NextRunAt = t.NextRunAt.UTC()
	return t, nil
}

// MaxAttempts is the attempt limit per task kind.
type MaxAttempts map[domain.TaskKind]int

func (m MaxAttempts) args() (start, settle, void int) {
	return m[domain.TaskStart], m[domain.TaskSettle], m[domain.TaskVoid]
}

const maxAttemptsSQL = `CASE task_type WHEN 'START' THEN $2::int WHEN 'SETTLE' THEN $3::int ELSE $4::int END`

// InsertTask enqueues a task due immediately. deadline is set only for START.
func (q *Queries) InsertTask(ctx context.Context, orderID uuid.UUID, kind domain.TaskKind, params any, createdAt time.Time, deadline *time.Time) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO tasks (task_id, order_id, task_type, parameters, created_at, next_run_at, deadline_at)
		VALUES ($1, $2, $3, $4, $5, $5, $6)`, uuid.New(), orderID, kind, params, createdAt, deadline)
	return err
}

// ClaimedTask is a task claimed for an ordinary attempt.
type ClaimedTask struct {
	domain.Task
	// Lag is the delay between next_run_at and the claim.
	Lag time.Duration
}

// ClaimTasks claims up to limit due tasks with a new claim_id and lease, incrementing
// attempt_count. START leases never extend past the deadline. Tasks without attempts
// left or with a passed deadline are left to the sweeps.
func (q *Queries) ClaimTasks(ctx context.Context, limit int, lease time.Duration, maxAttempts MaxAttempts) ([]ClaimedTask, error) {
	start, settle, void := maxAttempts.args()
	rows, err := q.db.Query(ctx, `
		WITH picked AS (
			SELECT task_id FROM tasks
			WHERE status = 'PENDING' AND next_run_at <= clock_timestamp()
				AND (lease_until IS NULL OR lease_until <= clock_timestamp())
				AND attempt_count < `+maxAttemptsSQL+`
				AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
			ORDER BY next_run_at, task_id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE tasks t
		SET claim_id = gen_random_uuid(),
			lease_until = LEAST(clock_timestamp() + make_interval(secs => $5), COALESCE(t.deadline_at, 'infinity')),
			attempt_count = t.attempt_count + 1
		FROM picked WHERE t.task_id = picked.task_id
		RETURNING t.task_id, t.order_id, t.task_type, t.status, t.parameters, t.attempt_count, t.claim_id,
			t.lease_until, t.next_run_at, t.deadline_at, extract(epoch FROM clock_timestamp() - t.next_run_at)::float8`,
		limit, start, settle, void, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var claimed []ClaimedTask
	for rows.Next() {
		var lagSeconds float64
		task, err := scanTask(rows, &lagSeconds)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, ClaimedTask{Task: task, Lag: time.Duration(lagSeconds * float64(time.Second))})
	}
	return claimed, rows.Err()
}

// LockTask locks a task by ID.
func (q *Queries) LockTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	return scanTask(q.db.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE task_id = $1 FOR UPDATE`, id))
}

// TryLockTask is LockTask that does not wait: a task locked by another
// transaction is reported as ErrNotFound.
func (q *Queries) TryLockTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	return scanTask(q.db.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE task_id = $1 FOR UPDATE SKIP LOCKED`, id))
}

// LockStartTask locks the START task of an order. Lock the order first.
func (q *Queries) LockStartTask(ctx context.Context, orderID uuid.UUID) (domain.Task, error) {
	return scanTask(q.db.QueryRow(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE order_id = $1 AND task_type = 'START' FOR UPDATE`, orderID))
}

// UpdateStartHoldParams corrects the HOLD amount under the first START claim's lock.
func (q *Queries) UpdateStartHoldParams(ctx context.Context, id uuid.UUID, params domain.HoldParams) error {
	_, err := q.db.Exec(ctx, `UPDATE tasks SET parameters = $2 WHERE task_id = $1`, id, params)
	return err
}

// CompleteTask finalizes a task, releases its claim and returns the persisted last error.
// An empty lastError keeps the previous one.
func (q *Queries) CompleteTask(ctx context.Context, id uuid.UUID, finishedAt time.Time, result string, reason *domain.Reason, lastError string) (string, error) {
	var persistedError string
	err := q.db.QueryRow(ctx, `
		UPDATE tasks SET status = 'COMPLETED', finished_at = $2, result = $3, completion_reason = $4,
			last_error = COALESCE(NULLIF($5, ''), last_error), claim_id = NULL, lease_until = NULL
		WHERE task_id = $1
		RETURNING COALESCE(last_error, '')`, id, finishedAt, result, reason, lastError).Scan(&persistedError)
	return persistedError, err
}

// RescheduleTask records a transient failure and releases the claim until nextRunAt.
func (q *Queries) RescheduleTask(ctx context.Context, id uuid.UUID, nextRunAt time.Time, lastError string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE tasks SET next_run_at = $2, last_error = $3, claim_id = NULL, lease_until = NULL
		WHERE task_id = $1`, id, nextRunAt, lastError)
	return err
}

// DeadLetterTask stops automatic attempts and returns the persisted last error.
func (q *Queries) DeadLetterTask(ctx context.Context, id uuid.UUID, lastError string) (string, error) {
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
	Kind     domain.TaskKind
	Position *time.Time
}

// FindStartsPastDeadline returns the next page of START tasks ordered by deadline and ID.
func (q *Queries) FindStartsPastDeadline(ctx context.Context, limit int, cutoff time.Time, after SweepTask) ([]SweepTask, error) {
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

// FindExhaustedTasks returns the next page of tasks with no attempts left and no live lease.
func (q *Queries) FindExhaustedTasks(ctx context.Context, limit int, maxAttempts MaxAttempts, cutoff time.Time, after SweepTask) ([]SweepTask, error) {
	start, settle, void := maxAttempts.args()
	rows, err := q.db.Query(ctx, `
		SELECT task_id, order_id, task_type, next_run_at FROM tasks
		WHERE status = 'PENDING' AND attempt_count >= `+maxAttemptsSQL+` AND created_at <= $7
			AND (lease_until IS NULL OR lease_until <= $7)
			AND (next_run_at, task_id) > (COALESCE($5::timestamptz, '-infinity'), $6::uuid)
		ORDER BY next_run_at, task_id LIMIT $1`, limit, start, settle, void, after.Position, after.ID, cutoff)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[SweepTask])
}

// DeadLetter is a task waiting for manual recovery.
type DeadLetter struct {
	ID           uuid.UUID
	OrderID      uuid.UUID
	Kind         domain.TaskKind
	AttemptCount int
	LastError    *string
}

func (q *Queries) ListDeadLetters(ctx context.Context) ([]DeadLetter, error) {
	rows, err := q.db.Query(ctx, `
		SELECT task_id, order_id, task_type, attempt_count, last_error FROM tasks
		WHERE status = 'DEAD_LETTER' ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[DeadLetter])
}

// RequeueDeadLetters returns dead-lettered tasks (one, or all when id is nil) to PENDING
// with a reset attempt count. Parameters and operation_id stay the same.
func (q *Queries) RequeueDeadLetters(ctx context.Context, id *uuid.UUID) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE tasks SET status = 'PENDING', attempt_count = 0, claim_id = NULL, lease_until = NULL,
			next_run_at = clock_timestamp()
		WHERE status = 'DEAD_LETTER' AND ($1::uuid IS NULL OR task_id = $1)`, id)
	return tag.RowsAffected(), err
}
