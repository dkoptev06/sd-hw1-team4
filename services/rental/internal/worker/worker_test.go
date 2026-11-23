package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/payment"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/testdb"
)

type fakePayments struct {
	mu      sync.Mutex
	results map[domain.TaskKind]payment.Result
	calls   map[domain.TaskKind]int
	during  func() // runs inside the call, before the answer
}

func (f *fakePayments) answer(kind domain.TaskKind) payment.Result {
	f.mu.Lock()
	f.calls[kind]++
	during := f.during
	result := f.results[kind]
	f.mu.Unlock()
	if during != nil {
		during()
	}
	return result
}

func (f *fakePayments) set(kind domain.TaskKind, r payment.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[kind] = r
}

func (f *fakePayments) count(kind domain.TaskKind) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[kind]
}

func (f *fakePayments) Hold(context.Context, domain.HoldParams) payment.Result {
	return f.answer(domain.TaskStart)
}
func (f *fakePayments) Settle(context.Context, domain.SettleParams) payment.Result {
	return f.answer(domain.TaskSettle)
}
func (f *fakePayments) Void(context.Context, domain.VoidParams) payment.Result {
	return f.answer(domain.TaskVoid)
}

type staticConfigs struct{}

func (staticConfigs) Current() dynconfig.Snapshot { return dynconfig.Defaults() }

const maxAttempts = 3

type env struct {
	store    *storage.Store
	payments *fakePayments
	worker   *Worker
}

func newEnv(t *testing.T) *env {
	t.Helper()
	store := testdb.New(t)
	payments := &fakePayments{
		results: map[domain.TaskKind]payment.Result{
			domain.TaskStart: {Outcome: payment.Success}, domain.TaskSettle: {Outcome: payment.Success},
			domain.TaskVoid: {Outcome: payment.Success},
		},
		calls: map[domain.TaskKind]int{},
	}
	cfg := settings.Worker{
		Concurrency: 4, BatchSize: 10, PollInterval: 10 * time.Millisecond, SweepInterval: 10 * time.Millisecond,
		Lease: 10 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute, ShutdownTimeout: time.Second,
		MaxAttempts: map[domain.TaskKind]int{domain.TaskStart: maxAttempts, domain.TaskSettle: maxAttempts, domain.TaskVoid: maxAttempts},
	}
	w := New(store, payments, staticConfigs{}, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.jitter = func() float64 { return 0 }
	return &env{store: store, payments: payments, worker: w}
}

// startingOrder inserts an offer, a STARTING order and its START task.
func (e *env) startingOrder(t *testing.T, deposit int64) domain.Order {
	t.Helper()
	ctx := context.Background()
	offer := domain.Offer{
		ID: uuid.New(), IdempotencyID: uuid.NewString(), UserID: "user",
		Scooter: domain.Scooter{ID: uuid.NewString(), ZoneID: "zone", Charge: 80},
		Terms:   domain.Terms{MinuteRate: 10, UnlockFee: 50, Deposit: deposit},
	}
	var order domain.Order
	err := e.store.InTx(ctx, func(q *storage.Queries) error {
		stored, _, err := q.InsertOffer(ctx, offer, time.Minute)
		if err != nil {
			return err
		}
		now, err := q.Now(ctx)
		if err != nil {
			return err
		}
		if err := q.ConsumeOffer(ctx, stored.ID); err != nil {
			return err
		}
		order = domain.NewOrder(stored, now)
		if err := q.InsertOrder(ctx, order); err != nil {
			return err
		}
		deadline := now.Add(2 * time.Minute)
		params := domain.HoldParams{OperationID: order.ID.String(), UserID: "user", Amount: deposit, HoldExpiresAt: deadline.Add(time.Hour)}
		return q.InsertTask(ctx, order.ID, domain.TaskStart, params, now, &deadline)
	})
	if err != nil {
		t.Fatal(err)
	}
	return order
}

// runOnce claims due tasks and executes them synchronously.
func (e *env) runOnce(t *testing.T) []storage.ClaimedTask {
	t.Helper()
	claimed := e.claim(t)
	for _, task := range claimed {
		e.worker.execute(context.Background(), task)
	}
	return claimed
}

func (e *env) claim(t *testing.T) []storage.ClaimedTask {
	t.Helper()
	claimed, err := e.store.ClaimTasks(context.Background(), 10, e.worker.cfg.Lease, e.worker.maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

type taskRow struct {
	Status   string
	Result   *string
	Reason   *string
	Attempts int
	Claimed  bool
	Error    *string
}

func (e *env) task(t *testing.T, orderID uuid.UUID, kind domain.TaskKind) (taskRow, bool) {
	t.Helper()
	var r taskRow
	err := e.store.Pool().QueryRow(context.Background(), `
		SELECT status, result, completion_reason, attempt_count, claim_id IS NOT NULL, last_error
		FROM tasks WHERE order_id = $1 AND task_type = $2`, orderID, kind).
		Scan(&r.Status, &r.Result, &r.Reason, &r.Attempts, &r.Claimed, &r.Error)
	if err != nil {
		return taskRow{}, false
	}
	return r, true
}

func (e *env) orderStatus(t *testing.T, id uuid.UUID) domain.OrderStatus {
	t.Helper()
	order, _, _, err := e.store.GetOrder(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return order.Status
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestStartOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		deposit    int64
		outcome    payment.Outcome
		wantOrder  domain.OrderStatus
		wantReason string
		wantHolds  int
		wantVoid   bool
	}{
		{"zero deposit skips HOLD", 0, payment.ContractError, domain.OrderActive, "", 0, false},
		{"hold success", 300, payment.Success, domain.OrderActive, "", 1, false},
		{"hold failed", 300, payment.Rejected, domain.OrderStartFailed, "PAYMENT_REJECTED", 1, false},
		{"unexpected 4xx", 300, payment.ContractError, domain.OrderStartFailed, "CONTRACT_ERROR", 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.payments.set(domain.TaskStart, payment.Result{Outcome: tt.outcome, Detail: "detail"})
			order := e.startingOrder(t, tt.deposit)
			e.runOnce(t)

			if got := e.orderStatus(t, order.ID); got != tt.wantOrder {
				t.Fatalf("order %s, want %s", got, tt.wantOrder)
			}
			start, _ := e.task(t, order.ID, domain.TaskStart)
			if start.Status != "COMPLETED" || str(start.Reason) != tt.wantReason || start.Claimed {
				t.Fatalf("START %+v", start)
			}
			_, hasVoid := e.task(t, order.ID, domain.TaskVoid)
			if hasVoid != tt.wantVoid || e.payments.count(domain.TaskStart) != tt.wantHolds {
				t.Fatalf("void=%t holds=%d", hasVoid, e.payments.count(domain.TaskStart))
			}
		})
	}
}

func TestTransientFailureIsRetried(t *testing.T) {
	e := newEnv(t)
	e.payments.set(domain.TaskStart, payment.Result{Outcome: payment.Transient, Detail: "status 503"})
	order := e.startingOrder(t, 300)
	e.runOnce(t)

	start, _ := e.task(t, order.ID, domain.TaskStart)
	if start.Status != "PENDING" || start.Attempts != 1 || start.Claimed || str(start.Error) != "status 503" {
		t.Fatalf("START after transient failure %+v", start)
	}
	if claimed := e.claim(t); len(claimed) != 0 {
		t.Fatal("retry claimed before its backoff")
	}
	testdb.Exec(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, order.ID)
	e.payments.set(domain.TaskStart, payment.Result{Outcome: payment.Success})
	e.runOnce(t)
	if got := e.orderStatus(t, order.ID); got != domain.OrderActive {
		t.Fatalf("order after retry %s", got)
	}
}

func TestStaleClaimIsIgnored(t *testing.T) {
	e := newEnv(t)
	order := e.startingOrder(t, 300)
	claimed := e.claim(t)
	// While HOLD is in flight the deadline passes and another participant closes the START.
	e.payments.during = func() {
		testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, order.ID)
		if err := e.worker.FinalizeStart(context.Background(), order.ID); err != nil {
			t.Error(err)
		}
	}
	e.worker.execute(context.Background(), claimed[0])

	if got := e.orderStatus(t, order.ID); got != domain.OrderStartFailed {
		t.Fatalf("late HOLD success reopened the order: %s", got)
	}
	start, _ := e.task(t, order.ID, domain.TaskStart)
	void, hasVoid := e.task(t, order.ID, domain.TaskVoid)
	if str(start.Reason) != "DEADLINE_EXCEEDED" || !hasVoid || void.Status != "PENDING" {
		t.Fatalf("START %+v VOID %+v", start, void)
	}
	if err := e.worker.FinalizeStart(context.Background(), order.ID); err != nil {
		t.Fatal(err)
	}
	var voids int
	if err := e.store.Pool().QueryRow(context.Background(), `SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID'`, order.ID).Scan(&voids); err != nil || voids != 1 {
		t.Fatalf("VOID count %d err=%v", voids, err)
	}
}

func TestSweepExhaustedStart(t *testing.T) {
	tests := []struct {
		name      string
		attempts  int
		leaseLive bool
		overdue   bool
		order     domain.OrderStatus
		reason    string
	}{
		{name: "attempts left", attempts: maxAttempts - 1, order: domain.OrderStarting},
		{name: "last attempt in flight", attempts: maxAttempts, leaseLive: true, order: domain.OrderStarting},
		{name: "last lease expired", attempts: maxAttempts, order: domain.OrderStartFailed, reason: "MAX_ATTEMPTS_EXCEEDED"},
		{name: "deadline beats lease", attempts: 1, leaseLive: true, overdue: true, order: domain.OrderStartFailed, reason: "DEADLINE_EXCEEDED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			order := e.startingOrder(t, 300)
			testdb.Exec(t, e.store, `
				UPDATE tasks SET attempt_count = $2, claim_id = gen_random_uuid(),
					lease_until = clock_timestamp() + CASE WHEN $3::bool THEN interval '1 minute' ELSE interval '-1 second' END,
					deadline_at = CASE WHEN $4::bool THEN clock_timestamp() ELSE deadline_at END,
					next_run_at = clock_timestamp() + interval '1 day'
				WHERE order_id = $1`, order.ID, tt.attempts, tt.leaseLive, tt.overdue)
			if err := e.worker.Sweep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := e.orderStatus(t, order.ID); got != tt.order {
				t.Fatalf("order %s, want %s", got, tt.order)
			}
			start, _ := e.task(t, order.ID, domain.TaskStart)
			_, hasVoid := e.task(t, order.ID, domain.TaskVoid)
			if str(start.Reason) != tt.reason || hasVoid != (tt.reason != "") || e.payments.count(domain.TaskStart) != 0 {
				t.Fatalf("START %+v void=%t", start, hasVoid)
			}
		})
	}
}

// One Sweep drains a backlog larger than a batch and skips orders locked by another
// transaction instead of waiting for them.
func TestSweepDrainsBacklog(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const total = 25 // more than two batches of 10
	orders := make([]domain.Order, total)
	for i := range orders {
		orders[i] = e.startingOrder(t, 300)
	}
	testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE task_type = 'START'`)

	locked := orders[0]
	err := e.store.InTx(ctx, func(q *storage.Queries) error {
		if _, err := q.LockOrder(ctx, locked.ID); err != nil {
			return err
		}
		return e.worker.Sweep(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range orders[1:] {
		if got := e.orderStatus(t, order.ID); got != domain.OrderStartFailed {
			t.Fatalf("order %s is %s after one sweep", order.ID, got)
		}
	}
	if got := e.orderStatus(t, locked.ID); got != domain.OrderStarting {
		t.Fatalf("locked order was closed: %s", got)
	}

	if err := e.worker.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.orderStatus(t, locked.ID); got != domain.OrderStartFailed {
		t.Fatalf("released order is %s", got)
	}
}

func TestStartClosedByAPI(t *testing.T) {
	e := newEnv(t)
	order := e.startingOrder(t, 300)
	// The API closed the expired STARTING order; the task is still pending.
	testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, order.ID)
	testdb.Exec(t, e.store, `
		UPDATE orders SET status = 'START_FAILED', closed_at = clock_timestamp(), delete_after = clock_timestamp(), cost = 0
		WHERE order_id = $1`, order.ID)
	if claimed := e.runOnce(t); len(claimed) != 0 {
		t.Fatal("a START past its deadline was claimed for an attempt")
	}
	if err := e.worker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	start, _ := e.task(t, order.ID, domain.TaskStart)
	_, hasVoid := e.task(t, order.ID, domain.TaskVoid)
	if start.Status != "COMPLETED" || str(start.Reason) != "DEADLINE_EXCEEDED" || !hasVoid || e.payments.count(domain.TaskStart) != 0 {
		t.Fatalf("START %+v void=%t", start, hasVoid)
	}
}

func (e *env) settleTask(t *testing.T) uuid.UUID {
	t.Helper()
	order := e.startingOrder(t, 0)
	testdb.Exec(t, e.store, `
		UPDATE orders SET status = 'COMPLETED', started_at = created_at, finish_time = clock_timestamp(),
			closed_at = clock_timestamp(), delete_after = clock_timestamp() + interval '1 year', cost = 100
		WHERE order_id = $1`, order.ID)
	testdb.Exec(t, e.store, `UPDATE tasks SET status = 'COMPLETED', finished_at = clock_timestamp() WHERE order_id = $1`, order.ID)
	err := e.store.InsertTask(context.Background(), order.ID, domain.TaskSettle,
		domain.SettleParams{OperationID: order.ID.String(), UserID: "user", TotalAmount: 100}, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return order.ID
}

func TestSettle(t *testing.T) {
	t.Run("failed means debt", func(t *testing.T) {
		e := newEnv(t)
		e.payments.set(domain.TaskSettle, payment.Result{Outcome: payment.Rejected})
		id := e.settleTask(t)
		e.runOnce(t)
		if settle, _ := e.task(t, id, domain.TaskSettle); settle.Status != "COMPLETED" || str(settle.Result) != "failed" {
			t.Fatalf("SETTLE %+v", settle)
		}
	})

	t.Run("4xx goes to DLQ and is requeued", func(t *testing.T) {
		e := newEnv(t)
		e.payments.set(domain.TaskSettle, payment.Result{Outcome: payment.ContractError, Detail: "status 409"})
		id := e.settleTask(t)
		e.runOnce(t)
		settle, _ := e.task(t, id, domain.TaskSettle)
		if settle.Status != "DEAD_LETTER" || str(settle.Error) != "status 409" || e.orderStatus(t, id) != domain.OrderCompleted {
			t.Fatalf("SETTLE %+v", settle)
		}
		if n, err := e.store.RequeueDeadLetters(context.Background(), nil); err != nil || n != 1 {
			t.Fatalf("requeue %d %v", n, err)
		}
		e.payments.set(domain.TaskSettle, payment.Result{Outcome: payment.Success})
		claimed := e.runOnce(t)
		if len(claimed) != 1 || claimed[0].AttemptCount != 1 {
			t.Fatalf("requeued task claimed %+v", claimed)
		}
		var params domain.SettleParams
		if err := json.Unmarshal(claimed[0].Params, &params); err != nil || params.OperationID != id.String() || params.TotalAmount != 100 {
			t.Fatalf("requeue changed parameters: %+v %v", params, err)
		}
		if settle, _ := e.task(t, id, domain.TaskSettle); settle.Status != "COMPLETED" {
			t.Fatalf("SETTLE after requeue %+v", settle)
		}
	})

	t.Run("exhausted transient failures go to DLQ", func(t *testing.T) {
		e := newEnv(t)
		e.payments.set(domain.TaskSettle, payment.Result{Outcome: payment.Transient, Detail: "timeout"})
		id := e.settleTask(t)
		for range maxAttempts {
			testdb.Exec(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, id)
			e.runOnce(t)
		}
		if claimed := e.runOnce(t); len(claimed) != 0 {
			t.Fatal("claimed past the attempt limit")
		}
		if err := e.worker.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if settle, _ := e.task(t, id, domain.TaskSettle); settle.Status != "DEAD_LETTER" || settle.Attempts != maxAttempts {
			t.Fatalf("SETTLE %+v", settle)
		}
	})
}

func TestRunProcessesQueue(t *testing.T) {
	e := newEnv(t)
	orders := make([]domain.Order, 5)
	for i := range orders {
		orders[i] = e.startingOrder(t, int64(i*100))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.worker.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for _, order := range orders {
		for e.orderStatus(t, order.ID) != domain.OrderActive {
			if time.Now().After(deadline) {
				t.Fatalf("order %s not activated", order.ID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
