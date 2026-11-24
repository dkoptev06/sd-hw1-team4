//go:build functional

package tests

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type StartTaskState struct {
	ID         string
	Status     string
	Reason     string
	Attempts   int
	ClaimID    string
	Parameters string
	Deadline   time.Time
	CreatedAt  time.Time
	NextRunAt  time.Time
	LeaseUntil sql.NullTime
	FinishedAt sql.NullTime
	Result     sql.NullString
	LastError  sql.NullString
}

func (s *Stand) ExpireStart(t *testing.T, orderID string) {
	t.Helper()
	// Сдвигаем дедлайн по часам PostgreSQL, чтобы не ждать две минуты.
	tag, err := s.DB.Exec(t.Context(), `UPDATE tasks SET deadline_at = clock_timestamp()
		WHERE order_id = $1 AND task_type = 'START' AND status = 'PENDING'`, orderID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("истечение дедлайна START: %s, %v", tag, err)
	}
}

func (s *Stand) RequirePendingStart(t *testing.T, orderID string) StartTaskState {
	t.Helper()
	start := s.ReadStart(t, orderID)
	if start.Status != "PENDING" || start.Reason != "" || start.Attempts != 0 || start.ClaimID != "" {
		t.Fatalf("новая задача START должна ожидать обработки: %+v", start)
	}
	return start
}

func (s *Stand) ReadStart(t *testing.T, orderID string) StartTaskState {
	t.Helper()
	var task StartTaskState
	err := s.DB.QueryRow(t.Context(), `
		SELECT task_id::text, status, COALESCE(completion_reason, ''), attempt_count,
			COALESCE(claim_id::text, ''), parameters::text, deadline_at, created_at, next_run_at,
			lease_until, finished_at, result, last_error
		FROM tasks WHERE order_id = $1 AND task_type = 'START'`, orderID).
		Scan(&task.ID, &task.Status, &task.Reason, &task.Attempts, &task.ClaimID, &task.Parameters, &task.Deadline,
			&task.CreatedAt, &task.NextRunAt, &task.LeaseUntil, &task.FinishedAt, &task.Result, &task.LastError)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Заказ %s, START %s, попыток %d, причина %q", orderID, task.Status, task.Attempts, task.Reason)
	return task
}

func (s *Stand) WaitForVoid(t *testing.T, orderID string) {
	t.Helper()
	WaitFor(t, "завершения VOID для "+orderID, func() (bool, error) {
		var completed bool
		err := s.DB.QueryRow(t.Context(), `
			SELECT EXISTS (SELECT 1 FROM tasks WHERE order_id = $1 AND task_type = 'VOID' AND status = 'COMPLETED')`,
			orderID).Scan(&completed)
		return completed, err
	})
}

func (s *Stand) RequireNoVoid(t *testing.T, orderID string) {
	t.Helper()
	var voids int
	err := s.DB.QueryRow(t.Context(),
		`SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID'`, orderID).Scan(&voids)
	if err != nil || voids != 0 {
		t.Fatalf("по заказу %s ожидалось 0 задач VOID, найдено %d, ошибка %v", orderID, voids, err)
	}
}

type PaymentTaskState struct {
	ID, Parameters, Result, LastError string
	Attempts                          int
	Claimed                           bool
}

func (s *Stand) WaitPaymentTask(t *testing.T, orderID, kind, status string) PaymentTaskState {
	t.Helper()
	var task PaymentTaskState
	WaitFor(t, kind+" в статусе "+status, func() (bool, error) {
		var got string
		err := s.DB.QueryRow(t.Context(), `
			SELECT status, task_id::text, parameters::text, COALESCE(result, ''), COALESCE(last_error, ''),
				attempt_count, claim_id IS NOT NULL OR lease_until IS NOT NULL
			FROM tasks WHERE order_id = $1 AND task_type = $2`, orderID, kind).
			Scan(&got, &task.ID, &task.Parameters, &task.Result, &task.LastError, &task.Attempts, &task.Claimed)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return got == status, err
	})
	t.Logf("Заказ %s, %s %s, попыток %d, результат %q", orderID, kind, status, task.Attempts, task.Result)
	return task
}
