//go:build functional

package finish_and_settle

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkSettleDebt(t *testing.T, rentalBinary, paymentBinary string) {
	t.Setenv("PAYMENT_SETTLE_RESULT", "failed")
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{})
	s.StartWorker(t)
	orderID := s.StartOrder(t)
	tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	finished := s.FinishTrip(t, orderID, false)
	task := s.WaitPaymentTask(t, orderID, "SETTLE", "COMPLETED")
	if task.Result != "failed" || task.Attempts != 1 || task.Claimed {
		t.Fatalf("SETTLE при долге: %+v", task)
	}
	s.RequirePayment(t, orderID, http.StatusOK, "DEBT")
	requireFinishReplay(t, s, finished)
	attempts := s.Proxy.RecordedAttempts()
	tests.RequirePaymentCommands(t, attempts, tests.PaymentCall{Method: "hold", OperationID: orderID},
		tests.PaymentCall{Method: "settle", OperationID: orderID})
	requireSettle(t, attempts[1], orderID, "user-1", *finished.Cost)
	replayed := tests.RequirePost[tests.PaymentStatus](t, s.Client, s.PaymentURL+"/payments/settle",
		settlement{OperationID: orderID, UserID: "user-1", TotalAmount: finished.Cost}, http.StatusOK)
	if replayed.Status != "failed" {
		t.Fatalf("Payment не сохранил результат failed: %+v", replayed)
	}
	s.RequirePayment(t, orderID, http.StatusOK, "DEBT")
}

func checkLostSettleResponse(t *testing.T, rentalBinary, paymentBinary, result, paymentStatus string) {
	t.Setenv("PAYMENT_SETTLE_RESULT", result)
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{DropSettle: true})
	s.StartWorker(t)
	orderID := s.StartOrder(t)
	tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	finished := s.FinishTrip(t, orderID, false)
	task := s.WaitPaymentTask(t, orderID, "SETTLE", "COMPLETED")
	if task.Result != result || task.Attempts != 2 || task.Claimed || task.LastError == "" {
		t.Fatalf("SETTLE после обрыва ответа: %+v", task)
	}
	if dropped := s.Proxy.DroppedResponses(); !slices.Equal(dropped, []string{"/payments/settle"}) {
		t.Fatalf("ожидался обрыв первого ответа SETTLE, получено %v", dropped)
	}
	attempts := s.Proxy.RecordedAttempts()
	settle := tests.PaymentCall{Method: "settle", OperationID: orderID}
	tests.RequirePaymentCommands(t, attempts, tests.PaymentCall{Method: "hold", OperationID: orderID}, settle, settle)
	for _, call := range attempts[1:] {
		requireSettle(t, call, orderID, "user-1", *finished.Cost)
		if call.Outcome != result {
			t.Fatalf("Payment вернул %q вместо %q", call.Outcome, result)
		}
	}
	s.RequirePayment(t, orderID, http.StatusOK, paymentStatus)
	requireFinishReplay(t, s, finished)
}

type settlement struct {
	OperationID string `json:"operation_id"`
	UserID      string `json:"user_id"`
	TotalAmount *int64 `json:"total_amount"`
}

func requireSettle(t *testing.T, attempt tests.PaymentAttempt, orderID, user string, cost int64) {
	t.Helper()
	var params settlement
	err := json.Unmarshal([]byte(attempt.Body), &params)
	if err != nil || attempt.Path != "/payments/settle" || params.OperationID != orderID ||
		params.UserID != user || params.TotalAmount == nil || *params.TotalAmount != cost {
		t.Fatalf("неожиданный SETTLE: %+v, ошибка %v", attempt, err)
	}
}
