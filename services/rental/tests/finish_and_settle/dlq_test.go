//go:build functional

package finish_and_settle

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkPaymentRetriesAndRequeue(t *testing.T, rentalBinary, paymentBinary string) {
	for _, kind := range []string{"SETTLE", "VOID"} {
		t.Run(kind+" возвращается из DLQ", func(t *testing.T) {
			var unavailable atomic.Bool
			unavailable.Store(true)
			method := strings.ToLower(kind)
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{
				ResponseStatus: func(path string) int {
					if path == "/payments/"+method && unavailable.Load() {
						return http.StatusServiceUnavailable
					}
					return 0
				},
			})
			workerEnv := []string{"SETTLE_MAX_ATTEMPTS=2", "VOID_MAX_ATTEMPTS=2"}
			deposit, user := int64(0), "trusted-user"
			if kind == "VOID" {
				deposit, user = 250, "user-1"
			}
			orderID := tests.StartTrip(t, s.Client, s.API, tests.OfferRequest{
				ScooterID: "scooter", UserID: user, IdempotencyID: "dlq",
			}, deposit)
			if kind == "VOID" {
				s.ExpireStart(t, orderID)
			}
			worker := s.StartWorker(t, workerEnv...)
			var finished tests.FinishResponse
			if kind == "SETTLE" {
				tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
				finished = s.FinishTrip(t, orderID, false)
			} else {
				tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
			}
			dead := s.WaitPaymentTask(t, orderID, kind, "DEAD_LETTER")
			if dead.Attempts != 2 || dead.Claimed || dead.LastError == "" {
				t.Fatalf("задача в DLQ имеет неожиданные поля: %+v", dead)
			}
			if kind == "SETTLE" {
				requireFinishReplay(t, s, finished)
			}

			worker.Stop()
			call := tests.PaymentCall{Method: method, OperationID: orderID}
			tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), call, call)
			listed := s.RentalCLI(t, "dlq", "list")
			if !strings.Contains(listed, dead.ID+"\t"+orderID+"\t"+kind+"\tattempts=2") {
				t.Fatalf("задача отсутствует в dlq list: %q", listed)
			}
			unavailable.Store(false)
			s.RentalCLI(t, "dlq", "requeue", dead.ID)
			pending := s.WaitPaymentTask(t, orderID, kind, "PENDING")
			if pending.ID != dead.ID || pending.Attempts != 0 || pending.Claimed || pending.Parameters != dead.Parameters {
				t.Fatalf("при возврате из DLQ изменились параметры или захват: было %+v, стало %+v", dead, pending)
			}

			s.StartWorker(t, workerEnv...)
			done := s.WaitPaymentTask(t, orderID, kind, "COMPLETED")
			if done.Result != "success" || done.Attempts != 1 || done.ID != dead.ID || done.Parameters != dead.Parameters {
				t.Fatalf("неожиданный результат после возврата из DLQ: %+v", done)
			}
			attempts := s.Proxy.RecordedAttempts()
			tests.RequirePaymentCommands(t, attempts, call, call, call)
			if kind == "SETTLE" {
				for _, attempt := range attempts {
					requireSettle(t, attempt, orderID, user, *finished.Cost)
				}
				s.RequirePayment(t, orderID, http.StatusOK, "PAID")
				requireFinishReplay(t, s, finished)
			} else {
				s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")
				tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
			}
		})
	}
}
