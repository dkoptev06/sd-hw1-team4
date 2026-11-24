//go:build functional

package task_claims_and_api_races

import (
	"slices"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkReplacementDuringHold(t *testing.T, rentalBinary, paymentBinary string) {
	for _, test := range []struct {
		name      string
		crash     bool
		dropReply bool
	}{
		{name: "API заменил заказ перед поздним успехом HOLD"},
		{name: "API заменил заказ перед ошибкой HOLD", dropReply: true},
		{name: "API заменил заказ перед падением worker", crash: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := tests.NewPaymentGate()
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{AfterHold: gate, DropHold: test.dropReply})
			first := s.StartWorker(t, "START_MAX_ATTEMPTS=1", "WORKER_SWEEP_INTERVAL=1h", "WORKER_CONCURRENCY=1",
				"PAYMENT_TIMEOUT=30s", "WORKER_LEASE=35s")
			oldID := s.StartOrder(t)
			gate.Wait(t)
			s.ExpireStart(t, oldID)
			before := s.ReadStart(t, oldID)
			if before.Status != "PENDING" || before.ClaimID == "" || before.Attempts != 1 {
				t.Fatalf("API должен заменить заказ при живом захвате последней попытки: %+v", before)
			}
			newID := tests.StartTrip(t, s.Client, s.API,
				tests.OfferRequest{ScooterID: "scooter", UserID: "user-1", IdempotencyID: "replacement"}, 250)
			replacement := s.RequirePendingStart(t, newID)
			// До проверки старого ответа первый worker должен видеть только его HOLD.
			tag, err := s.DB.Exec(t.Context(), `
				UPDATE tasks SET next_run_at = deadline_at WHERE task_id = $1
					AND status = 'PENDING' AND attempt_count = 0 AND claim_id IS NULL`, replacement.ID)
			if err != nil || tag.RowsAffected() != 1 {
				t.Fatalf("отложить START нового заказа: %s, %v", tag, err)
			}
			tests.WaitForOrder(t, s.Client, s.API, oldID, "START_FAILED")
			if after := s.ReadStart(t, oldID); after != before {
				t.Fatalf("API изменил старую задачу START: было %+v, стало %+v", before, after)
			}
			s.RequireNoVoid(t, oldID)

			if !test.crash {
				gate.Release()
				outcome := "success"
				if test.dropReply {
					outcome = "transient"
				}
				waitForHoldOutcome(t, s, first, outcome)
				tests.WaitForStaleStart(t, s.Client, first.URL)
				if after := s.ReadStart(t, oldID); after != before {
					t.Fatalf("поздний ответ изменил старую задачу START: было %+v, стало %+v", before, after)
				}
				s.RequireNoVoid(t, oldID)
			}
			first.Stop()
			tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), tests.PaymentCall{Method: "hold", OperationID: oldID})
			tag, err = s.DB.Exec(t.Context(), `UPDATE tasks SET next_run_at = $2 WHERE task_id = $1`, replacement.ID, replacement.NextRunAt)
			if err != nil || tag.RowsAffected() != 1 {
				t.Fatalf("восстановить расписание START нового заказа: %s, %v", tag, err)
			}
			s.StartWorker(t, "START_MAX_ATTEMPTS=1")
			s.RequireFailedStart(t, oldID, "DEADLINE_EXCEEDED", 1)
			tests.WaitForOrder(t, s.Client, s.API, newID, "ACTIVE")
			tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(),
				tests.PaymentCall{Method: "hold", OperationID: oldID}, tests.PaymentCall{Method: "void", OperationID: oldID},
				tests.PaymentCall{Method: "hold", OperationID: newID})
			if test.dropReply && !slices.Equal(s.Proxy.DroppedResponses(), []string{"/payments/hold"}) {
				t.Fatalf("ожидался потерянный ответ HOLD: %v", s.Proxy.DroppedResponses())
			}
		})
	}
}
