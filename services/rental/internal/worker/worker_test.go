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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

type fakePayments struct {
	mu      sync.Mutex
	results map[task.Kind]task.Result
	calls   map[task.Kind]int
	holds   []task.HoldParams
	during  func() // runs inside the call, before the answer
}

func (f *fakePayments) answer(kind task.Kind) task.Result {
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

func (f *fakePayments) set(kind task.Kind, r task.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[kind] = r
}

func (f *fakePayments) count(kind task.Kind) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[kind]
}

func (f *fakePayments) Hold(_ context.Context, params task.HoldParams) task.Result {
	f.mu.Lock()
	f.holds = append(f.holds, params)
	f.mu.Unlock()
	return f.answer(task.Start)
}
func (f *fakePayments) Settle(context.Context, task.SettleParams) task.Result {
	return f.answer(task.Settle)
}
func (f *fakePayments) Void(context.Context, task.VoidParams) task.Result {
	return f.answer(task.Void)
}

type staticConfigs struct{}

func (staticConfigs) Current() dynconfig.Snapshot { return dynconfig.Defaults() }

const maxAttempts = 3

type env struct {
	store    *pgxpool.Pool
	payments *fakePayments
	worker   *Worker
	orders   *order.Service
	tasks    *task.Service
}

func newEnv(t *testing.T) *env {
	t.Helper()
	store := tests.NewDB(t)
	payments := &fakePayments{
		results: map[task.Kind]task.Result{
			task.Start: {Outcome: task.Success}, task.Settle: {Outcome: task.Success},
			task.Void: {Outcome: task.Success},
		},
		calls: map[task.Kind]int{},
	}
	cfg := Options{
		Concurrency: 4, BatchSize: 10, PollInterval: 10 * time.Millisecond, SweepInterval: 10 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	tasks := task.NewService(store, task.Options{
		Lease: 10 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute, Jitter: func() float64 { return 0 },
		MaxAttempts: task.MaxAttempts{task.Start: maxAttempts, task.Settle: maxAttempts, task.Void: maxAttempts},
	})
	orders := order.NewService(store, nil, tasks, nil, staticConfigs{}, 2*time.Minute, time.Hour, log)
	w := New(orders, tasks, payments, cfg, log)
	return &env{store: store, orders: orders, tasks: tasks, payments: payments, worker: w}
}

// startingOrder inserts an offer, a STARTING order and its START task.
func (e *env) startingOrder(t *testing.T, deposit int64) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	offerID := uuid.New()
	scooter := source.Scooter{ID: uuid.NewString(), ZoneID: "zone", Charge: 80}
	terms := offer.Terms{MinuteRate: 10, UnlockFee: 50, Deposit: deposit}
	orderID := uuid.New()
	err := postgres.WithTx(ctx, e.store, func(tx postgres.Tx) error {
		now, err := postgres.Now(ctx, tx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge,
				minute_rate, unlock_fee, deposit, user_profile_fallback_used, status, created_at, expires_at)
			VALUES ($1, $2, 'user', $3, $4, $5, $6, $7, $8, false, 'CONSUMED', $9, $10)`,
			offerID, uuid.NewString(), scooter.ID, scooter.ZoneID, scooter.Charge, terms.MinuteRate,
			terms.UnlockFee, terms.Deposit, now, now.Add(time.Minute)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
   INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate, unlock_fee, deposit, status, created_at)
   VALUES ($1, $2, 'user', $3, $4, $5, $6, $7, 'STARTING', $8)`,
			orderID, offerID, scooter.ID, scooter.ZoneID, terms.MinuteRate, terms.UnlockFee, terms.Deposit, now); err != nil {
			return err
		}
		return e.tasks.EnqueueStart(ctx, tx, orderID, "user", deposit, now, 2*time.Minute, time.Hour)
	})
	if err != nil {
		t.Fatal(err)
	}
	return orderID
}

// runOnce claims due tasks and executes them synchronously.
func (e *env) runOnce(t *testing.T) []task.Claim {
	t.Helper()
	claimed := e.claim(t)
	for _, task := range claimed {
		e.worker.execute(context.Background(), task)
	}
	return claimed
}

func (e *env) claim(t *testing.T) []task.Claim {
	t.Helper()
	claimed, err := e.tasks.Claim(context.Background(), 10)
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

func (e *env) task(t *testing.T, orderID uuid.UUID, kind task.Kind) (taskRow, bool) {
	t.Helper()
	var r taskRow
	err := e.store.QueryRow(context.Background(), `
		SELECT status, result, completion_reason, attempt_count, claim_id IS NOT NULL, last_error
		FROM tasks WHERE order_id = $1 AND task_type = $2`, orderID, kind).
		Scan(&r.Status, &r.Result, &r.Reason, &r.Attempts, &r.Claimed, &r.Error)
	if err != nil {
		return taskRow{}, false
	}
	return r, true
}

func (e *env) orderStatus(t *testing.T, id uuid.UUID) order.Status {
	t.Helper()
	view, err := e.orders.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return view.Status
}

func (e *env) finalizeStart(ctx context.Context, id uuid.UUID) error {
	event, err := e.orders.FinalizeStart(ctx, id, nil)
	if err == nil {
		e.worker.report(event)
	}
	return err
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

func requireTaskLog(t *testing.T, log *bytes.Buffer, orderID uuid.UUID, kind task.Kind, status, lastError string) {
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
		outcome    task.Outcome
		wantOrder  order.Status
		wantReason string
		wantHolds  int
		wantVoid   bool
	}{
		{"zero deposit skips HOLD", 0, task.ContractError, order.Active, "", 0, false},
		{"hold success", 300, task.Success, order.Active, "", 1, false},
		{"hold failed", 300, task.Rejected, order.StartFailed, "PAYMENT_REJECTED", 1, false},
		{"unexpected 4xx", 300, task.ContractError, order.StartFailed, "CONTRACT_ERROR", 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.payments.set(task.Start, task.Result{Outcome: tt.outcome, Detail: "detail"})
			orderID := e.startingOrder(t, tt.deposit)
			e.runOnce(t)

			if got := e.orderStatus(t, orderID); got != tt.wantOrder {
				t.Fatalf("order %s, want %s", got, tt.wantOrder)
			}
			start, _ := e.task(t, orderID, task.Start)
			if start.Status != "COMPLETED" || str(start.Reason) != tt.wantReason || start.Claimed {
				t.Fatalf("START %+v", start)
			}
			_, hasVoid := e.task(t, orderID, task.Void)
			if hasVoid != tt.wantVoid || e.payments.count(task.Start) != tt.wantHolds {
				t.Fatalf("void=%t holds=%d", hasVoid, e.payments.count(task.Start))
			}
		})
	}
}

func TestTransientFailureIsRetried(t *testing.T) {
	e := newEnv(t)
	log := e.captureLog()
	e.payments.set(task.Start, task.Result{Outcome: task.Transient, Detail: "status 503"})
	orderID := e.startingOrder(t, 300)
	e.runOnce(t)

	start, _ := e.task(t, orderID, task.Start)
	if start.Status != "PENDING" || start.Attempts != 1 || start.Claimed || str(start.Error) != "status 503" {
		t.Fatalf("START after transient failure %+v", start)
	}
	if claimed := e.claim(t); len(claimed) != 0 {
		t.Fatal("retry claimed before its backoff")
	}
	tests.ExecSQL(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, orderID)
	e.payments.set(task.Start, task.Result{Outcome: task.Success})
	e.runOnce(t)
	if got := e.orderStatus(t, orderID); got != order.Active {
		t.Fatalf("order after retry %s", got)
	}
	if start, _ := e.task(t, orderID, task.Start); start.Status != "COMPLETED" || str(start.Error) != "status 503" {
		t.Fatalf("completed START %+v", start)
	}
	requireTaskLog(t, log, orderID, task.Start, "COMPLETED", "status 503")
}

func TestStartUsesOrderDeposit(t *testing.T) {
	t.Run("positive deposit corrects first HOLD and keeps it on retry", func(t *testing.T) {
		e := newEnv(t)
		orderID := e.startingOrder(t, 300)
		tests.ExecSQL(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '0') WHERE order_id = $1`, orderID)
		e.payments.set(task.Start, task.Result{Outcome: task.Transient, Detail: "timeout"})
		e.payments.during = func() {
			var amount int64
			err := e.store.QueryRow(t.Context(),
				`SELECT (parameters ->> 'amount')::bigint FROM tasks WHERE order_id = $1`, orderID).Scan(&amount)
			if err != nil || amount != 300 {
				t.Fatalf("HOLD amount should be recorded before Payment runs, got %d (%v)", amount, err)
			}
		}
		e.runOnce(t)
		e.payments.during = nil

		var userID string
		var createdAt time.Time
		if err := e.store.QueryRow(t.Context(), `SELECT user_id, created_at FROM orders WHERE order_id = $1`, orderID).Scan(&userID, &createdAt); err != nil {
			t.Fatal(err)
		}
		holds := e.payments.holds
		if len(holds) != 1 || holds[0].Amount != 300 || holds[0].OperationID != orderID.String() ||
			holds[0].UserID != userID || !holds[0].HoldExpiresAt.Equal(createdAt.Add(time.Hour+2*time.Minute)) {
			t.Fatalf("first HOLD should keep its saved identifiers and expiration, with the order deposit: %+v", holds)
		}

		tests.ExecSQL(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, orderID)
		e.payments.set(task.Start, task.Result{Outcome: task.Success})
		e.runOnce(t)
		holds = e.payments.holds
		if len(holds) != 2 || holds[0] != holds[1] || e.orderStatus(t, orderID) != order.Active {
			t.Fatalf("retry should use the first HOLD parameters and activate the order: %+v", holds)
		}
	})

	t.Run("zero deposit skips a first HOLD", func(t *testing.T) {
		e := newEnv(t)
		orderID := e.startingOrder(t, 0)
		tests.ExecSQL(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '300') WHERE order_id = $1`, orderID)
		e.runOnce(t)
		if calls := e.payments.count(task.Start); calls != 0 || e.orderStatus(t, orderID) != order.Active {
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
			orderID := e.startingOrder(t, test.deposit)
			tests.ExecSQL(t, e.store, `UPDATE tasks SET parameters = jsonb_set(parameters, '{amount}', '200'),
				attempt_count = 1 WHERE order_id = $1`, orderID)
			e.runOnce(t)

			start, _ := e.task(t, orderID, task.Start)
			_, hasVoid := e.task(t, orderID, task.Void)
			if start.Status != "COMPLETED" || str(start.Reason) != "CONTRACT_ERROR" ||
				!strings.Contains(str(start.Error), "reconcile in Payment") || hasVoid != test.wantVoid ||
				e.orderStatus(t, orderID) != order.StartFailed || e.payments.count(task.Start) != 0 {
				t.Fatalf("incompatible HOLD retry must stop and be recorded: START %+v, VOID=%t", start, hasVoid)
			}
			requireTaskLog(t, log, orderID, task.Start, "COMPLETED", str(start.Error))
		})
	}
}

func TestStartDeadlineLogsLastError(t *testing.T) {
	e := newEnv(t)
	log := e.captureLog()
	e.payments.set(task.Start, task.Result{Outcome: task.Transient, Detail: "timeout"})
	orderID := e.startingOrder(t, 300)
	e.runOnce(t)
	tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, orderID)
	if err := e.finalizeStart(t.Context(), orderID); err != nil {
		t.Fatal(err)
	}
	start, _ := e.task(t, orderID, task.Start)
	if start.Status != "COMPLETED" || str(start.Reason) != "DEADLINE_EXCEEDED" || str(start.Error) != "timeout" {
		t.Fatalf("expired START %+v", start)
	}
	requireTaskLog(t, log, orderID, task.Start, "COMPLETED", "timeout")
}

func TestStartSkipsHoldForInvalidClaim(t *testing.T) {
	for _, test := range []struct {
		name      string
		beforeRun func(*testing.T, *env, uuid.UUID)
	}{
		{name: "deadline passed before call", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, id)
		}},
		{name: "start already completed", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, id)
			if err := e.finalizeStart(t.Context(), id); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "another claim replaced this one", beforeRun: func(t *testing.T, e *env, id uuid.UUID) {
			tests.ExecSQL(t, e.store, `UPDATE tasks SET claim_id = gen_random_uuid() WHERE order_id = $1`, id)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := newEnv(t)
			orderID := e.startingOrder(t, 300)
			claimed := e.claim(t)
			if len(claimed) != 1 {
				t.Fatalf("START claims: %+v", claimed)
			}
			test.beforeRun(t, e, orderID)
			e.worker.execute(t.Context(), claimed[0])
			if calls := e.payments.count(task.Start); calls != 0 {
				t.Fatalf("sent %d HOLD calls with an invalid START claim", calls)
			}
		})
	}
}

func TestStaleClaimIsIgnored(t *testing.T) {
	e := newEnv(t)
	orderID := e.startingOrder(t, 300)
	claimed := e.claim(t)
	// While HOLD is in flight the deadline passes and another participant closes the START.
	e.payments.during = func() {
		tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, orderID)
		if err := e.finalizeStart(context.Background(), orderID); err != nil {
			t.Error(err)
		}
	}
	e.worker.execute(context.Background(), claimed[0])

	if got := e.orderStatus(t, orderID); got != order.StartFailed {
		t.Fatalf("late HOLD success reopened the order: %s", got)
	}
	start, _ := e.task(t, orderID, task.Start)
	void, hasVoid := e.task(t, orderID, task.Void)
	if str(start.Reason) != "DEADLINE_EXCEEDED" || !hasVoid || void.Status != "PENDING" {
		t.Fatalf("START %+v VOID %+v", start, void)
	}
	if err := e.finalizeStart(context.Background(), orderID); err != nil {
		t.Fatal(err)
	}
	var voids int
	if err := e.store.QueryRow(context.Background(), `SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'VOID'`, orderID).Scan(&voids); err != nil || voids != 1 {
		t.Fatalf("VOID count %d err=%v", voids, err)
	}
}

func TestSweepExhaustedStart(t *testing.T) {
	cases := []struct {
		name      string
		attempts  int
		leaseLive bool
		overdue   bool
		order     order.Status
		reason    string
	}{
		{name: "attempts left", attempts: maxAttempts - 1, order: order.Starting},
		{name: "last attempt in flight", attempts: maxAttempts, leaseLive: true, order: order.Starting},
		{name: "last lease expired", attempts: maxAttempts, order: order.StartFailed, reason: "MAX_ATTEMPTS_EXCEEDED"},
		{name: "deadline beats lease", attempts: 1, leaseLive: true, overdue: true, order: order.StartFailed, reason: "DEADLINE_EXCEEDED"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			orderID := e.startingOrder(t, 300)
			tests.ExecSQL(t, e.store, `
				UPDATE tasks SET attempt_count = $2, claim_id = gen_random_uuid(),
					lease_until = clock_timestamp() + CASE WHEN $3::bool THEN interval '1 minute' ELSE interval '-1 second' END,
					deadline_at = CASE WHEN $4::bool THEN clock_timestamp() ELSE deadline_at END,
					next_run_at = clock_timestamp() + interval '1 day'
				WHERE order_id = $1`, orderID, tt.attempts, tt.leaseLive, tt.overdue)
			if err := e.worker.Sweep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := e.orderStatus(t, orderID); got != tt.order {
				t.Fatalf("order %s, want %s", got, tt.order)
			}
			start, _ := e.task(t, orderID, task.Start)
			_, hasVoid := e.task(t, orderID, task.Void)
			if str(start.Reason) != tt.reason || hasVoid != (tt.reason != "") || e.payments.count(task.Start) != 0 {
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
	orders := make([]uuid.UUID, total)
	for i := range orders {
		orders[i] = e.startingOrder(t, 300)
	}
	tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE task_type = 'START'`)

	locked := orders[0]
	err := postgres.WithTx(ctx, e.store, func(tx postgres.Tx) error {
		var id uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT order_id FROM orders WHERE order_id = $1 FOR UPDATE`, locked).Scan(&id); err != nil {
			return err
		}
		return e.worker.Sweep(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, orderID := range orders[1:] {
		if got := e.orderStatus(t, orderID); got != order.StartFailed {
			t.Fatalf("order %s is %s after one sweep", orderID, got)
		}
	}
	if got := e.orderStatus(t, locked); got != order.Starting {
		t.Fatalf("locked order was closed: %s", got)
	}

	if err := e.worker.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.orderStatus(t, locked); got != order.StartFailed {
		t.Fatalf("released order is %s", got)
	}
}

func TestSweepContinuesAfterFailedTask(t *testing.T) {
	e := newEnv(t)
	e.worker.cfg.BatchSize = 1
	first := e.startingOrder(t, 250)
	second := e.startingOrder(t, 0)
	tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = created_at;
		ALTER TABLE tasks ADD CONSTRAINT injected_void_failure CHECK (task_type <> 'VOID')`)
	if err := e.worker.Sweep(t.Context()); err == nil || !strings.Contains(err.Error(), "injected_void_failure") {
		t.Fatalf("expected failure to insert the first order's VOID, got %v", err)
	}
	if e.orderStatus(t, first) != order.Starting || e.orderStatus(t, second) != order.StartFailed {
		t.Fatal("the failed START must roll back while the next page still completes")
	}
}

func TestSweepDefersNewWorkToNextPass(t *testing.T) {
	e := newEnv(t)
	e.worker.cfg.BatchSize = 1
	first := e.startingOrder(t, 250)
	second := e.startingOrder(t, 0)
	tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = created_at WHERE order_id = $1`, first)
	// Closing the first START causes another deadline to pass and inserts an
	// exhausted VOID with a future schedule while the sweep is between pages.
	tests.ExecSQL(t, e.store, `
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
	void, _ := e.task(t, first, task.Void)
	if void.Status != "PENDING" || e.orderStatus(t, second) != order.Starting {
		t.Fatalf("first pass picked up new work: VOID %+v, order %s", void, e.orderStatus(t, second))
	}
	if err := e.worker.Sweep(t.Context()); err != nil {
		t.Fatal(err)
	}
	void, _ = e.task(t, first, task.Void)
	if void.Status != "DEAD_LETTER" || e.orderStatus(t, second) != order.StartFailed {
		t.Fatalf("second pass missed new work: VOID %+v, order %s", void, e.orderStatus(t, second))
	}
}

func TestStartClosedByAPI(t *testing.T) {
	e := newEnv(t)
	orderID := e.startingOrder(t, 300)
	// The API closed the expired STARTING order; the task is still pending.
	tests.ExecSQL(t, e.store, `UPDATE tasks SET deadline_at = clock_timestamp() WHERE order_id = $1`, orderID)
	tests.ExecSQL(t, e.store, `
		UPDATE orders SET status = 'START_FAILED', closed_at = clock_timestamp(), delete_after = clock_timestamp(), cost = 0
		WHERE order_id = $1`, orderID)
	if claimed := e.runOnce(t); len(claimed) != 0 {
		t.Fatal("a START past its deadline was claimed for an attempt")
	}
	if err := e.worker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	start, _ := e.task(t, orderID, task.Start)
	_, hasVoid := e.task(t, orderID, task.Void)
	if start.Status != "COMPLETED" || str(start.Reason) != "DEADLINE_EXCEEDED" || !hasVoid || e.payments.count(task.Start) != 0 {
		t.Fatalf("START %+v void=%t", start, hasVoid)
	}
}

func (e *env) settleTask(t *testing.T) uuid.UUID {
	t.Helper()
	orderID := e.startingOrder(t, 0)
	tests.ExecSQL(t, e.store, `
		UPDATE orders SET status = 'COMPLETED', started_at = created_at, finish_time = clock_timestamp(),
			closed_at = clock_timestamp(), delete_after = clock_timestamp() + interval '1 year', cost = 100
		WHERE order_id = $1`, orderID)
	tests.ExecSQL(t, e.store, `UPDATE tasks SET status = 'COMPLETED', finished_at = clock_timestamp() WHERE order_id = $1`, orderID)
	ctx := t.Context()
	now, err := postgres.Now(ctx, e.store)
	if err != nil {
		t.Fatal(err)
	}
	err = postgres.WithTx(ctx, e.store, func(tx postgres.Tx) error {
		return e.tasks.EnqueueSettle(ctx, tx, orderID, "user", big.NewInt(100), now)
	})
	if err != nil {
		t.Fatal(err)
	}
	return orderID
}

// Both candidates share a scheduling timestamp. The task ID breaks the tie
// when the first page is locked.
func TestSweepSkipsLockedTasks(t *testing.T) {
	for _, kind := range []task.Kind{task.Start, task.Settle} {
		t.Run(string(kind), func(t *testing.T) {
			e := newEnv(t)
			e.worker.cfg.BatchSize = 1
			for range 2 {
				if kind == task.Start {
					e.startingOrder(t, 250)
				} else {
					e.settleTask(t)
				}
			}
			tests.ExecSQL(t, e.store, `UPDATE tasks SET attempt_count = $2, next_run_at = statement_timestamp()
				WHERE task_type = $1`, kind, maxAttempts)
			rows, err := e.store.Query(t.Context(), `SELECT task_id, order_id FROM tasks
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
			err = postgres.WithTx(ctx, e.store, func(tx postgres.Tx) error {
				var id uuid.UUID
				if err := tx.QueryRow(ctx, `SELECT task_id FROM tasks WHERE task_id = $1 FOR UPDATE`, locked.ID).Scan(&id); err != nil {
					return err
				}
				return e.worker.Sweep(ctx)
			})
			if err != nil {
				t.Fatal(err)
			}
			want := "DEAD_LETTER"
			if kind == task.Start {
				want = "COMPLETED"
			}
			blocked, _ := e.task(t, locked.OrderID, kind)
			free, _ := e.task(t, other.OrderID, kind)
			if blocked.Status != "PENDING" || free.Status != want || free.Attempts != maxAttempts {
				t.Fatalf("after one sweep: locked %+v, other %+v, want PENDING and %s", blocked, free, want)
			}
			if kind == task.Start && str(free.Reason) != "MAX_ATTEMPTS_EXCEEDED" {
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
		e.payments.set(task.Settle, task.Result{Outcome: task.Rejected})
		id := e.settleTask(t)
		e.runOnce(t)
		if settle, _ := e.task(t, id, task.Settle); settle.Status != "COMPLETED" || str(settle.Result) != "failed" {
			t.Fatalf("SETTLE %+v", settle)
		}
	})

	t.Run("4xx goes to DLQ and is requeued", func(t *testing.T) {
		e := newEnv(t)
		e.payments.set(task.Settle, task.Result{Outcome: task.ContractError, Detail: "status 409"})
		id := e.settleTask(t)
		e.runOnce(t)
		settle, _ := e.task(t, id, task.Settle)
		if settle.Status != "DEAD_LETTER" || str(settle.Error) != "status 409" || e.orderStatus(t, id) != order.Completed {
			t.Fatalf("SETTLE %+v", settle)
		}
		if n, err := e.tasks.RequeueDeadLetters(context.Background(), nil); err != nil || n != 1 {
			t.Fatalf("requeue %d %v", n, err)
		}
		e.payments.set(task.Settle, task.Result{Outcome: task.Success})
		claimed := e.runOnce(t)
		if len(claimed) != 1 || claimed[0].AttemptCount() != 1 {
			t.Fatalf("requeued task claimed %+v", claimed)
		}
		if params, err := e.tasks.SettleParams(claimed[0]); err != nil || params.OperationID != id.String() ||
			params.TotalAmount == nil || params.TotalAmount.Cmp(big.NewInt(100)) != 0 {
			t.Fatalf("requeue changed parameters: %+v %v", params, err)
		}
		if settle, _ := e.task(t, id, task.Settle); settle.Status != "COMPLETED" {
			t.Fatalf("SETTLE after requeue %+v", settle)
		}
	})

	t.Run("exhausted transient failures go to DLQ", func(t *testing.T) {
		e := newEnv(t)
		log := e.captureLog()
		e.payments.set(task.Settle, task.Result{Outcome: task.Transient, Detail: "timeout"})
		id := e.settleTask(t)
		for range maxAttempts {
			tests.ExecSQL(t, e.store, `UPDATE tasks SET next_run_at = clock_timestamp() WHERE order_id = $1`, id)
			e.runOnce(t)
		}
		if claimed := e.runOnce(t); len(claimed) != 0 {
			t.Fatal("claimed past the attempt limit")
		}
		if err := e.worker.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if settle, _ := e.task(t, id, task.Settle); settle.Status != "DEAD_LETTER" ||
			settle.Attempts != maxAttempts || str(settle.Error) != "timeout" {
			t.Fatalf("SETTLE %+v", settle)
		}
		requireTaskLog(t, log, id, task.Settle, "DEAD_LETTER", "timeout")
	})
}

func TestRunProcessesQueue(t *testing.T) {
	e := newEnv(t)
	orders := make([]uuid.UUID, 5)
	for i := range orders {
		orders[i] = e.startingOrder(t, int64(i*100))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.worker.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for _, orderID := range orders {
		for e.orderStatus(t, orderID) != order.Active {
			if time.Now().After(deadline) {
				t.Fatalf("order %s not activated", orderID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
