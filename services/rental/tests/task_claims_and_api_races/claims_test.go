//go:build functional

package task_claims_and_api_races

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkFinalAttempt(t *testing.T, rentalBinary, paymentBinary string, lastAttempt []string) {
	for _, test := range []struct {
		name  string
		crash bool
	}{
		{name: "Последняя попытка успешно учитывается"},
		{name: "Падение во время последней попытки", crash: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := tests.NewPaymentGate()
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{AfterHold: gate})
			first := s.StartWorker(t, lastAttempt...)
			orderID := s.StartOrder(t)
			gate.Wait(t)
			s.RequirePayment(t, orderID, http.StatusOK, "HELD")
			claim := s.ReadStart(t, orderID)
			if claim.Status != "PENDING" || claim.Attempts != 1 || claim.ClaimID == "" || !claim.LeaseUntil.Valid {
				t.Fatalf("ожидалась захваченная последняя попытка: %+v", claim)
			}
			if test.crash {
				first.Stop()
				s.StartWorker(t, lastAttempt...)
				s.RequireFailedStart(t, orderID, "MAX_ATTEMPTS_EXCEEDED", 1)
				done := s.ReadStart(t, orderID)
				if !done.FinishedAt.Valid || done.FinishedAt.Time.Before(claim.LeaseUntil.Time) {
					t.Fatalf("START закрыт в %v до истечения lease %v", done.FinishedAt, claim.LeaseUntil)
				}
				tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(),
					tests.PaymentCall{Method: "hold", OperationID: orderID}, tests.PaymentCall{Method: "void", OperationID: orderID})
				return
			}

			gate.Release()
			tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
			s.RequireNoVoid(t, orderID)
			if attempt := s.ReadStart(t, orderID); attempt.Status != "COMPLETED" ||
				attempt.Reason != "" || attempt.Attempts != 1 || attempt.ClaimID != "" {
				t.Fatalf("итог успешной последней попытки: %+v", attempt)
			}
			tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), tests.PaymentCall{Method: "hold", OperationID: orderID})
		})
	}
}

func checkLateHold(t *testing.T, rentalBinary, paymentBinary string, expireLease bool) {
	gate := tests.NewPaymentGate()
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{AfterHold: gate})
	first := s.StartWorker(t, "START_MAX_ATTEMPTS=1", "WORKER_SWEEP_INTERVAL=1h", "PAYMENT_TIMEOUT=30s", "WORKER_LEASE=35s")
	orderID := s.StartOrder(t)
	gate.Wait(t)
	tag, err := s.DB.Exec(t.Context(), `
		UPDATE tasks SET deadline_at = clock_timestamp(),
			lease_until = CASE WHEN $2 THEN clock_timestamp() ELSE lease_until END,
			next_run_at = clock_timestamp() + interval '1 day'
		WHERE order_id = $1 AND task_type = 'START' AND claim_id IS NOT NULL`, orderID, expireLease)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("подготовка истёкшего захвата: %s, %v", tag, err)
	}

	claim := s.ReadStart(t, orderID)
	second := s.StartWorker(t, "START_MAX_ATTEMPTS=1")
	s.RequireFailedStart(t, orderID, "DEADLINE_EXCEEDED", 1)
	done := s.ReadStart(t, orderID)
	if !done.FinishedAt.Valid || !done.FinishedAt.Time.Before(claim.NextRunAt) {
		t.Fatalf("START должен закрыться до next_run_at: до %+v, после %+v", claim, done)
	}
	if !expireLease && (!claim.LeaseUntil.Valid || !done.FinishedAt.Time.Before(claim.LeaseUntil.Time)) {
		t.Fatalf("START должен закрыться при действующем lease: до %+v, после %+v", claim, done)
	}
	tests.WaitForMetric(t, s.Client, second.URL, `rental_tasks_finished_total{kind="START",reason="DEADLINE_EXCEEDED",status="COMPLETED"} 1`)
	gate.Release()
	waitForHoldOutcome(t, s, first, "success")
	tests.WaitForStaleStart(t, s.Client, first.URL)
	if after := s.ReadStart(t, orderID); after != done {
		t.Fatalf("поздний HOLD изменил завершённый START: было %+v, стало %+v", done, after)
	}
	s.RequireFailedStart(t, orderID, "DEADLINE_EXCEEDED", 1)
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(),
		tests.PaymentCall{Method: "hold", OperationID: orderID}, tests.PaymentCall{Method: "void", OperationID: orderID})
}

func checkReclaimedStart(t *testing.T, rentalBinary, paymentBinary string) {
	firstReply := tests.NewPaymentGate()
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{AfterHold: firstReply})
	workerEnv := []string{"START_MAX_ATTEMPTS=2", "WORKER_CONCURRENCY=1", "PAYMENT_TIMEOUT=30s", "WORKER_LEASE=35s"}
	first := s.StartWorker(t, workerEnv...)
	orderID := s.StartOrder(t)
	firstReply.Wait(t)
	initial := s.ReadStart(t, orderID)
	if initial.Status != "PENDING" || initial.Attempts != 1 || initial.ClaimID == "" || !initial.LeaseUntil.Valid {
		t.Fatalf("ожидался первый захват START: %+v", initial)
	}

	secondReply := tests.NewPaymentGate()
	secondProxy := tests.NewPaymentProxy(t, s.PaymentURL, tests.PaymentFaults{AfterHold: secondReply})
	second := s.StartWorker(t, append(workerEnv, "PAYMENT_URL="+secondProxy.URL)...)
	// Имитируем истечение первого lease до ответа, сохраняя исходный дедлайн START.
	tag, err := s.DB.Exec(t.Context(), `
		UPDATE tasks SET lease_until = clock_timestamp()
		WHERE task_id = $1 AND claim_id = $2 AND status = 'PENDING' AND deadline_at > clock_timestamp()`,
		initial.ID, initial.ClaimID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("истечение первого lease: %s, %v", tag, err)
	}
	secondReply.Wait(t)
	current := s.ReadStart(t, orderID)
	if current.Status != "PENDING" || current.Attempts != 2 || current.ClaimID == "" ||
		current.ClaimID == initial.ClaimID || !current.LeaseUntil.Valid || !current.Deadline.Equal(initial.Deadline) {
		t.Fatalf("ожидался новый захват той же START: до %+v, после %+v", initial, current)
	}

	firstReply.Release()
	waitForHoldOutcome(t, s, first, "success")
	tests.WaitFor(t, "отклонения ответа первого worker", func() (bool, error) {
		if got := s.ReadStart(t, orderID); got != current {
			return false, fmt.Errorf("прежний worker изменил START с новым claim_id: было %+v, стало %+v", current, got)
		}
		stale, err := tests.MetricValue(s.Client, first.URL, `rental_task_stale_claims_total{kind="START"}`)
		return stale == 1, err
	})
	var now time.Time
	if err := s.DB.QueryRow(t.Context(), "SELECT clock_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	if !now.Before(current.Deadline) || !now.Before(current.LeaseUntil.Time) {
		t.Fatalf("к моменту отклонения ответа истёк дедлайн или новый lease: время %v, задача %+v", now, current)
	}
	starting := tests.WaitForOrder(t, s.Client, s.API, orderID, "STARTING")
	if starting.StartedAt != nil || starting.FinishTime != nil || starting.Cost != nil {
		t.Fatalf("прежний worker изменил заказ до ответа нового worker: %+v", starting)
	}

	secondReply.Release()
	waitForHoldOutcome(t, s, second, "success")
	active := tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	done := s.ReadStart(t, orderID)
	if done.Status != "COMPLETED" || done.Result.String != "success" || done.Reason != "" ||
		done.Attempts != 2 || done.ClaimID != "" || done.LeaseUntil.Valid ||
		active.StartedAt == nil || !active.StartedAt.Before(current.Deadline) {
		t.Fatalf("новый worker не завершил START вовремя: заказ %+v, задача %+v", active, done)
	}
	s.RequireNoVoid(t, orderID)

	firstAttempts, secondAttempts := s.Proxy.RecordedAttempts(), secondProxy.RecordedAttempts()
	hold := tests.PaymentCall{Method: "hold", OperationID: orderID}
	tests.RequirePaymentCommands(t, firstAttempts, hold)
	tests.RequirePaymentCommands(t, secondAttempts, hold)
	if firstAttempts[0].Body != secondAttempts[0].Body {
		t.Fatalf("повторный захват изменил параметры HOLD: %s и %s", firstAttempts[0].Body, secondAttempts[0].Body)
	}
}

func waitForHoldOutcome(t *testing.T, s *tests.PaymentStand, worker *tests.ServiceProcess, outcome string) {
	t.Helper()
	tests.WaitForMetric(t, s.Client, worker.URL,
		fmt.Sprintf(`rental_dependency_calls_total{dependency="payment",operation="hold",outcome=%q} 1`, outcome))
}

func checkBlockedOrder(t *testing.T, rentalBinary, paymentBinary string) {
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{})
	first := s.StartOrder(t)
	second := tests.StartTrip(t, s.Client, s.API,
		tests.OfferRequest{ScooterID: "other-scooter", UserID: "user-2", IdempotencyID: "other-offer"}, 250)
	s.ExpireStart(t, first)
	s.ExpireStart(t, second)

	lock, err := s.DB.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(t.Context())
	if _, err := lock.Exec(t.Context(), "SELECT order_id FROM orders WHERE order_id = $1 FOR UPDATE", first); err != nil {
		t.Fatal(err)
	}
	worker := s.StartWorker(t, "WORKER_BATCH_SIZE=1")
	s.RequireFailedStart(t, second, "DEADLINE_EXCEEDED", 0)
	tests.WaitForOrder(t, s.Client, s.API, first, "STARTING")
	s.RequirePendingStart(t, first)
	s.RequireNoVoid(t, first)
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), tests.PaymentCall{Method: "void", OperationID: second})
	const deadlineCount = `rental_tasks_finished_total{kind="START",reason="DEADLINE_EXCEEDED",status="COMPLETED"}`
	tests.WaitForMetric(t, s.Client, worker.URL, deadlineCount+" 1")
	t.Logf("При заблокированном заказе %s его START остался PENDING. Заказ %s перешёл из STARTING в START_FAILED, VOID завершён, %s = 1", first, second, deadlineCount)

	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.RequireFailedStart(t, first, "DEADLINE_EXCEEDED", 0)
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), tests.PaymentCall{Method: "void", OperationID: second},
		tests.PaymentCall{Method: "void", OperationID: first})
	tests.WaitForMetric(t, s.Client, worker.URL, deadlineCount+" 2")
	t.Logf("После снятия блокировки заказ %s перешёл в START_FAILED, VOID завершён, %s = 2", first, deadlineCount)
}
