//go:build functional

package hold_void_and_lost_responses

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkVoidAndLateHold(t *testing.T, rentalBinary, paymentBinary string) {
	for _, test := range []struct {
		name      string
		holdFirst bool
	}{
		{name: "HOLD выполнен до VOID, ответ VOID потерян", holdFirst: true},
		{name: "Первый HOLD доставлен после VOID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gate := tests.NewPaymentGate()
			faults := tests.PaymentFaults{}
			if test.holdFirst {
				faults.AfterHold, faults.DropVoid = gate, true
			} else {
				faults.BeforeHold = gate
			}
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, faults)
			worker := s.StartWorker(t, "PAYMENT_TIMEOUT=30s", "WORKER_LEASE=35s")
			orderID := s.StartOrder(t)

			gate.Wait(t)
			if test.holdFirst {
				s.RequirePayment(t, orderID, http.StatusOK, "HELD")
			} else {
				s.RequirePayment(t, orderID, http.StatusNotFound, "NOT_FOUND")
			}
			s.ExpireStart(t, orderID)
			tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
			s.WaitForVoid(t, orderID)
			s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")

			gate.Release()
			outcome := "rejected"
			if test.holdFirst {
				outcome = "success"
			}
			tests.WaitForMetric(t, s.Client, worker.URL,
				fmt.Sprintf(`rental_dependency_calls_total{dependency="payment",operation="hold",outcome=%q} 1`, outcome))
			tests.WaitForStaleStart(t, s.Client, worker.URL)
			tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
			s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")
			start := s.ReadStart(t, orderID)
			if start.Status != "COMPLETED" || start.Reason != "DEADLINE_EXCEEDED" || start.ClaimID != "" {
				t.Fatalf("неожиданный итог просроченного START: %+v", start)
			}

			attempts := s.Proxy.RecordedAttempts()
			wantVoids, wantHold := 1, "failed"
			if test.holdFirst {
				wantVoids, wantHold = 2, "success"
			}
			if len(attempts) != 1+wantVoids || attempts[0].Path != "/payments/hold" || attempts[0].Outcome != wantHold {
				t.Fatalf("неожиданное выполнение HOLD и VOID: %+v", attempts)
			}
			readHold(t, attempts[0], orderID)
			for _, attempt := range attempts[1:] {
				var params struct {
					OperationID string `json:"operation_id"`
				}
				if err := json.Unmarshal([]byte(attempt.Body), &params); err != nil ||
					attempt.Path != "/payments/void" || attempt.Outcome != "success" || params.OperationID != orderID {
					t.Fatalf("неожиданный VOID: %+v, %v", attempt, err)
				}
			}
			if test.holdFirst && !slices.Equal(s.Proxy.DroppedResponses(), []string{"/payments/void"}) {
				t.Fatalf("ожидалась потеря первого ответа VOID, получено %v", s.Proxy.DroppedResponses())
			}
		})
	}
}

func checkHoldExpiresWithVoidUnavailable(t *testing.T, rentalBinary, paymentBinary string) {
	gate := tests.NewPaymentGate()
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary,
		tests.PaymentFaults{AfterHold: gate, ResponseStatus: func(path string) int {
			if path == "/payments/void" {
				return http.StatusServiceUnavailable
			}
			return 0
		}}, "START_TTL=2s", "HOLD_GRACE=2s")
	s.StartWorker(t, "VOID_MAX_ATTEMPTS=100")
	orderID := s.StartOrder(t)
	gate.Wait(t)
	s.RequirePayment(t, orderID, http.StatusOK, "HELD")
	expiry := readHold(t, s.Proxy.RecordedAttempts()[0], orderID).HoldExpiresAt
	wantExpiry := s.ReadStart(t, orderID).Deadline.Add(2 * time.Second)
	if !expiry.Equal(wantExpiry) {
		t.Fatalf("hold_expires_at = %v, ожидалось %v", expiry, wantExpiry)
	}

	s.ExpireStart(t, orderID)
	tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	gate.Release()
	tests.WaitFor(t, "попытки недоступного VOID", func() (bool, error) {
		return len(s.Proxy.RecordedAttempts()) >= 2, nil
	})
	s.RequirePayment(t, orderID, http.StatusOK, "HELD")
	tests.WaitFor(t, "истечения удержания без VOID", func() (bool, error) {
		status, body, err := s.PaymentStatus(orderID)
		return status == http.StatusOK && body.Status == "HOLD_EXPIRED", err
	})
	if time.Now().Before(expiry) {
		t.Fatalf("удержание истекло до переданного hold_expires_at %v", expiry)
	}
	for _, attempt := range s.Proxy.RecordedAttempts()[1:] {
		if attempt.Path != "/payments/void" || attempt.Outcome != "" {
			t.Fatalf("запрос после HOLD дошёл до Payment или оказался вне сценария: %+v", attempt)
		}
	}
}
