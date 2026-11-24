//go:build functional

package hold_void_and_lost_responses

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkHoldRecovery(t *testing.T, rentalBinary, paymentBinary string) {
	for _, test := range []struct {
		name  string
		crash bool
	}{
		{name: "Падение worker после HOLD", crash: true},
		{name: "Потерян ответ HOLD"},
	} {
		t.Run(test.name, func(t *testing.T) {
			faults := tests.PaymentFaults{DropHold: !test.crash}
			if test.crash {
				faults.AfterHold = tests.NewPaymentGate()
			}
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, faults)
			workerEnv := []string{"PAYMENT_TIMEOUT=2s", "WORKER_LEASE=3s"}
			first := s.StartWorker(t, workerEnv...)
			orderID := s.StartOrder(t)
			if test.crash {
				faults.AfterHold.Wait(t)
				s.RequirePayment(t, orderID, http.StatusOK, "HELD")
				starting := tests.WaitForOrder(t, s.Client, s.API, orderID, "STARTING")
				if starting.StartedAt != nil || starting.FinishTime != nil || starting.Cost != nil {
					t.Fatalf("заказ активирован до ответа HOLD: %+v", starting)
				}
				first.Stop()
				s.StartWorker(t, workerEnv...)
			}

			active := tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
			s.RequirePayment(t, orderID, http.StatusOK, "HELD")
			attempts := s.Proxy.RecordedAttempts()
			if len(attempts) != 2 || attempts[0].Outcome != "success" || attempts[1].Outcome != "success" {
				t.Fatalf("ожидались два успешных HOLD без GET, вызовы %+v", attempts)
			}
			initial, repeated := readHold(t, attempts[0], orderID), readHold(t, attempts[1], orderID)
			if initial.UserID != repeated.UserID || !initial.HoldExpiresAt.Equal(repeated.HoldExpiresAt) {
				t.Fatalf("повтор HOLD изменил параметры: %+v и %+v", initial, repeated)
			}
			if !test.crash && !slices.Equal(s.Proxy.DroppedResponses(), []string{"/payments/hold"}) {
				t.Fatalf("ожидалась потеря первого ответа HOLD, получено %v", s.Proxy.DroppedResponses())
			}
			start := s.ReadStart(t, orderID)
			if start.Status != "COMPLETED" || start.Reason != "" || start.Attempts != 2 ||
				start.ClaimID != "" || start.LeaseUntil.Valid || !start.FinishedAt.Valid || start.Result.String != "success" {
				t.Fatalf("неожиданный итог повторного START: %+v", start)
			}
			if !test.crash && start.LastError.String == "" {
				t.Fatal("START должен сохранить техническую ошибку потери ответа HOLD")
			}
			wantExpiry := start.Deadline.Add(time.Hour)
			if !initial.HoldExpiresAt.Equal(wantExpiry) {
				t.Fatalf("hold_expires_at = %v, ожидалось %v", initial.HoldExpiresAt, wantExpiry)
			}
			if active.StartedAt == nil || !active.StartedAt.Before(start.Deadline) || active.FinishTime != nil || active.Cost == nil {
				t.Fatalf("неожиданное состояние активированного заказа: %+v", active)
			}
			s.RequireNoVoid(t, orderID)
		})
	}
}

func checkHoldRejected(t *testing.T, rentalBinary, paymentBinary string) {
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{})
	orderID := s.StartOrder(t)
	s.RequirePendingStart(t, orderID)

	// Предварительный VOID в Payment заставляет первый HOLD вернуть явный failed.
	void := tests.RequirePost[tests.PaymentStatus](t, s.Client, s.PaymentURL+"/payments/void",
		map[string]string{"operation_id": orderID}, http.StatusOK)
	if void.Status != "success" {
		t.Fatalf("не удалось подготовить отклонение HOLD: %+v", void)
	}

	s.StartWorker(t)
	order := tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	if order.Cost == nil || *order.Cost != 0 || order.StartedAt != nil || order.FinishTime != nil {
		t.Fatalf("неожиданный заказ после явного отказа: %+v", order)
	}
	start := s.ReadStart(t, orderID)
	if start.Status != "COMPLETED" || start.Result.String != "failed" || start.Reason != "PAYMENT_REJECTED" ||
		start.Attempts != 1 || start.ClaimID != "" || start.LeaseUntil.Valid || start.LastError.Valid ||
		!start.FinishedAt.Valid || !start.FinishedAt.Time.Before(start.Deadline) {
		t.Fatalf("неожиданный START после явного отказа: %+v", start)
	}
	s.RequireNoVoid(t, orderID)
	s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")
	attempts := s.Proxy.RecordedAttempts()
	tests.RequirePaymentCommands(t, attempts, tests.PaymentCall{Method: "hold", OperationID: orderID})
	readHold(t, attempts[0], orderID)
	if attempts[0].Outcome != "failed" {
		t.Fatalf("первый HOLD должен вернуть failed: %+v", attempts[0])
	}
}

type holdParams struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
	Amount        int64     `json:"amount"`
}

func readHold(t *testing.T, attempt tests.PaymentAttempt, orderID string) holdParams {
	t.Helper()
	var params holdParams
	err := json.Unmarshal([]byte(attempt.Body), &params)
	if err != nil || attempt.Path != "/payments/hold" || params.OperationID != orderID ||
		params.UserID != "user-1" || params.Amount != 250 || params.HoldExpiresAt.IsZero() {
		t.Fatalf("неожиданные параметры HOLD: %s %+v, %v", attempt.Path, params, err)
	}
	return params
}
