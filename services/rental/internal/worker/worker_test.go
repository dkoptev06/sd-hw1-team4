package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
	holds   []domain.HoldParams
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

func (f *fakePayments) Hold(_ context.Context, params domain.HoldParams) payment.Result {
	f.mu.Lock()
	f.holds = append(f.holds, params)
	f.mu.Unlock()
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

func (e *env) captureLog() *bytes.Buffer {
	var log bytes.Buffer
	e.worker.log = slog.New(slog.NewJSONHandler(&log, nil))
	return &log
}

func requireTaskLog(t *testing.T, log *bytes.Buffer, orderID uuid.UUID, kind domain.TaskKind, status, lastError string) {
	t.Helper()
	var event struct {
		OrderID   string `json:"order_id"`
		Kind      string `json:"kind"`
		Status    string `json:"status"`
		LastError string `json:"last_error"`
	}
	if err := json.Unmarshal(log.Bytes(), &event); err != nil || event.OrderID != orderID.String() ||
		event.Kind != string(kind) || event.Status != status || event.LastError != lastError {
		t.Fatalf("%s log for %s should contain %q: %s (%v)", status, orderID, lastError, log, err)
	}
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
	log := e.captureLog()
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
	if start, _ := e.task(t, order.ID, domain.TaskStart); start.Status != "COMPLETED" || str(start.Error) != "status 503" {
		t.Fatalf("completed START %+v", start)
	}
	requireTaskLog(t, log, order.ID, domain.TaskStart, "COMPLETED", "status 503")
}

func TestStartUsesOrderDeposit(t *testing.T) {
	t.Run("positive deposit corrects first HOLD and keeps it on retry", func(t *testing.T) {
		e := newEnv(t)
		order := e.startingOrder(t, 300)
		testdb.Exec(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '0') WHERE order_id = $1`, order.ID)
		e.payments.set(domain.TaskStart, payment.Result{Outcome: payment.Transient, Detail: "timeout"})
		e.payments.during = func() {
			var amount int64
			err := e.store.Pool().QueryRow(t.Context(),
				`SELECT (parameters ->> 'amount')::bigint FROM tasks WHERE order_id = $1`, order.ID).Scan(&amount)
			if err != nil || amount != 300 {
				t.Fatalf("HOLD amount should be recorded before Payment runs, got %d (%v)", amount, err)
			}
		}
		e.runOnce(t)
		e.payments.during = nil

		holds := e.payments.holds
		if len(holds) != 1 || holds[0].Amount != 300 || holds[0].OperationID != order.ID.String() ||
			holds[0].UserID != order.UserID || !holds[0].HoldExpiresAt.Equal(order.CreatedAt.Add(time.Hour+2*time.Minute)) {
			t.Fatalf("first HOLD should keep its saved identifiers and expiration, with the order deposit: %+v", holds)
		}

		testdb.Exec(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, order.ID)
		e.payments.set(domain.TaskStart, payment.Result{Outcome: payment.Success})
		e.runOnce(t)
		holds = e.payments.holds
		if len(holds) != 2 || holds[0] != holds[1] || e.orderStatus(t, order.ID) != domain.OrderActive {
			t.Fatalf("retry should use the first HOLD parameters and activate the order: %+v", holds)
		}
	})

	t.Run("zero deposit skips a first HOLD", func(t *testing.T) {
		e := newEnv(t)
		order := e.startingOrder(t, 0)
		testdb.Exec(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '300') WHERE order_id = $1`, order.ID)
		e.runOnce(t)
		if calls := e.payments.count(domain.TaskStart); calls != 0 || e.orderStatus(t, order.ID) != domain.OrderActive {
			t.Fatalf("zero deposit should activate the order without HOLD, got %d calls", calls)
		}
	})

	for _, test := range []struct {
		name     string
		deposit  int64
		wantVoid bool
	}{
		{name: "positive deposit uses VOID", deposit: 300, wantVoid: true},
		{name: "zero deposit records the need to reconcile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t)
			log := e.captureLog()
			order := e.startingOrder(t, test.deposit)
			testdb.Exec(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '200'),
				attempt_count = 1 WHERE order_id = $1`, order.ID)
			e.runOnce(t)

			start, _ := e.task(t, order.ID, domain.TaskStart)
			_, hasVoid := e.task(t, order.ID, domain.TaskVoid)
			if start.Status != "COMPLETED" || str(start.Reason) != "CONTRACT_ERROR" ||
				!strings.Contains(str(start.Error), "reconcile in Payment") || hasVoid != test.wantVoid ||
				e.orderStatus(t, order.ID) != domain.OrderStartFailed || e.payments.count(domain.TaskStart) != 0 {
				t.Fatalf("incompatible HOLD retry must stop and be recorded: START %+v, VOID=%t", start, hasVoid)
			}
			requireTaskLog(t, log, order.ID, domain.TaskStart, "COMPLETED", str(start.Error))
		})
	}
}

func TestStartDeadlineLogsLastError(t *testing.T) {
	e := newEnv(t)
	log := e.captureLog()
	e.payments.set(domain.TaskStart, payment.Result{Outcome: payment.Transient, Detail: "timeout"})
	order := e.startingOrder(t, 300)
	e.runOnce(t)
	testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, order.ID)
	if err := e.worker.FinalizeStart(t.Context(), order.ID); err != nil {
		t.Fatal(err)
	}
	start, _ := e.task(t, order.ID, domain.TaskStart)
	if start.Status != "COMPLETED" || str(start.Reason) != "DEADLINE_EXCEEDED" || str(start.Error) != "timeout" {
		t.Fatalf("expired START %+v", start)
	}
	requireTaskLog(t, log, order.ID, domain.TaskStart, "COMPLETED", "timeout")
}

func TestStartSkipsHoldForInvalidClaim(t *testing.T) {
	for _, test := range []struct {
		name      string
		beforeRun func(*testing.T, *env, uuid.UUID)
	}{
		{name: "deadline passed before call", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, id)
		}},
		{name: "start already completed", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, id)
			if err := e.worker.FinalizeStart(t.Context(), id); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "another claim replaced this one", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			testdb.Exec(t, e.store, `UPDATE tasks SET claim_id = gen_random_uuid() WHERE order_id = $1`, id)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t)
			order := e.startingOrder(t, 300)
			claimed := e.claim(t)
			if len(claimed) != 1 {
				t.Fatalf("START claims: %+v", claimed)
			}
			test.beforeRun(t, e, order.ID)
			e.worker.execute(t.Context(), claimed[0])
			if calls := e.payments.count(domain.TaskStart); calls != 0 {
				t.Fatalf("sent %d HOLD calls with an invalid START claim", calls)
			}
		})
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

func TestSweepContinuesAfterFailedTask(t *testing.T) {
	e := newEnv(t)
	e.worker.cfg.BatchSize = 1
	first := e.startingOrder(t, 250)
	second := e.startingOrder(t, 0)
	testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = created_at;
		ALTER TABLE tasks ADD CONSTRAINT injected_void_failure CHECK (task_type <> 'VOID')`)
	if err := e.worker.Sweep(t.Context()); err == nil || !strings.Contains(err.Error(), "injected_void_failure") {
		t.Fatalf("expected failure to insert the first order's VOID, got %v", err)
	}
	if e.orderStatus(t, first.ID) != domain.OrderStarting || e.orderStatus(t, second.ID) != domain.OrderStartFailed {
		t.Fatal("the failed START must roll back while the next page still completes")
	}
}

func TestSweepDefersNewWorkToNextPass(t *testing.T) {
	e := newEnv(t)
	e.worker.cfg.BatchSize = 1
	first := e.startingOrder(t, 250)
	second := e.startingOrder(t, 0)
	testdb.Exec(t, e.store, `UPDATE tasks SET deadline_at = created_at WHERE order_id = $1`, first.ID)
	// Closing the first START causes another deadline to pass and inserts an
	// exhausted VOID with a future schedule while the sweep is between pages.
	testdb.Exec(t, e.store, `
		CREATE FUNCTION add_work_during_sweep() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			UPDATE tasks SET deadline_at = clock_timestamp()
			WHERE task_type = 'START' AND order_id <> NEW.order_id;
			NEW.attempt_count := 3;
			NEW.next_run_at := clock_timestamp() + interval '1 day';
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER add_work BEFORE INSERT ON tasks FOR EACH ROW
			WHEN (NEW.task_type = 'VOID') EXECUTE FUNCTION add_work_during_sweep()`)
	if err := e.worker.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	void, _ := e.task(t, first.ID, domain.TaskVoid)
	if void.Status != "PENDING" || e.orderStatus(t, second.ID) != domain.OrderStarting {
		t.Fatalf("first pass picked up new work: VOID %+v, order %s", void, e.orderStatus(t, second.ID))
	}
	if err := e.worker.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	void, _ = e.task(t, first.ID, domain.TaskVoid)
	if void.Status != "DEAD_LETTER" || e.orderStatus(t, second.ID) != domain.OrderStartFailed {
		t.Fatalf("second pass missed new work: VOID %+v, order %s", void, e.orderStatus(t, second.ID))
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
	ctx := t.Context()
	now, err := e.store.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = e.store.InsertTask(ctx, order.ID, domain.TaskSettle,
		domain.SettleParams{OperationID: order.ID.String(), UserID: "user", TotalAmount: big.NewInt(100)}, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	return order.ID
}

// Both candidates share a scheduling timestamp. The task ID breaks the tie
// when the first page is locked.
func TestSweepSkipsLockedTasks(t *testing.T) {
	for _, kind := range []domain.TaskKind{domain.TaskStart, domain.TaskSettle} {
		t.Run(string(kind), func(t *testing.T) {
			e := newEnv(t)
			e.worker.cfg.BatchSize = 1
			for range 2 {
				if kind == domain.TaskStart {
					e.startingOrder(t, 250)
				} else {
					e.settleTask(t)
				}
			}
			testdb.Exec(t, e.store, `UPDATE tasks SET attempt_count = $2, next_run_at = statement_timestamp()
				WHERE task_type = $1`, kind, maxAttempts)
			rows, err := e.store.Pool().Query(t.Context(), `SELECT task_id, order_id FROM tasks
				WHERE task_type = $1 ORDER BY next_run_at, task_id`, kind)
			if err != nil {
				t.Fatal(err)
			}
			type taskRef struct{ ID, OrderID uuid.UUID }
			tasks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[taskRef])
			if err != nil || len(tasks) != 2 {
				t.Fatalf("prepare exhausted tasks: %v, %v", tasks, err)
			}
			locked, other := tasks[0], tasks[1]
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			// Hold the lock until the sweep has returned. Waiting for this task
			// instead of skipping it would hit the context deadline.
			err = e.store.InTx(ctx, func(q *storage.Queries) error {
				if _, err := q.LockTask(ctx, locked.ID); err != nil {
					return err
				}
				return e.worker.Sweep(ctx)
			})
			if err != nil {
				t.Fatal(err)
			}
			want := "DEAD_LETTER"
			if kind == domain.TaskStart {
				want = "COMPLETED"
			}
			blocked, _ := e.task(t, locked.OrderID, kind)
			free, _ := e.task(t, other.OrderID, kind)
			if blocked.Status != "PENDING" || free.Status != want || free.Attempts != maxAttempts {
				t.Fatalf("after one sweep: locked %+v, other %+v, want PENDING and %s", blocked, free, want)
			}
			if kind == domain.TaskStart && str(free.Reason) != "MAX_ATTEMPTS_EXCEEDED" {
				t.Fatalf("unexpected START reason: %+v", free)
			}
			if err := e.worker.Sweep(ctx); err != nil {
				t.Fatal(err)
			}
			if released, _ := e.task(t, locked.OrderID, kind); released.Status != want {
				t.Fatalf("released %s: %+v", kind, released)
			}
		})
	}
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
		if err := json.Unmarshal(claimed[0].Params, &params); err != nil || params.OperationID != id.String() ||
			params.TotalAmount == nil || params.TotalAmount.Cmp(big.NewInt(100)) != 0 {
			t.Fatalf("requeue changed parameters: %+v %v", params, err)
		}
		if settle, _ := e.task(t, id, domain.TaskSettle); settle.Status != "COMPLETED" {
			t.Fatalf("SETTLE after requeue %+v", settle)
		}
	})

	t.Run("exhausted transient failures go to DLQ", func(t *testing.T) {
		e := newEnv(t)
		log := e.captureLog()
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
		if settle, _ := e.task(t, id, domain.TaskSettle); settle.Status != "DEAD_LETTER" ||
			settle.Attempts != maxAttempts || str(settle.Error) != "timeout" {
			t.Fatalf("SETTLE %+v", settle)
		}
		requireTaskLog(t, log, id, domain.TaskSettle, "DEAD_LETTER", "timeout")
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
