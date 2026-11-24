//go:build functional

package payment_contract_errors

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkHoldContractError(t *testing.T, rentalBinary, paymentBinary string, response contractResponse) {
	s, _ := standWithContractError(t, rentalBinary, paymentBinary, "hold")
	orderID := s.StartOrder(t)
	worker := startContractErrorWorker(t, s, response)
	call := tests.PaymentCall{Method: "hold", OperationID: orderID}
	s.RequireFailedStart(t, orderID, "CONTRACT_ERROR", 1)
	task := s.WaitPaymentTask(t, orderID, "START", "COMPLETED")
	requireContractError(t, task.LastError, response)
	tests.WaitForMetric(t, s.Client, worker.URL,
		`rental_tasks_finished_total{kind="START",reason="CONTRACT_ERROR",status="COMPLETED"} 1`)
	waitForNextWorkerPoll(t, s, orderID, "START")
	if after := s.WaitPaymentTask(t, orderID, "START", "COMPLETED"); after != task {
		t.Fatalf("после следующего цикла worker START изменился: было %+v, стало %+v", task, after)
	}
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), call, tests.PaymentCall{Method: "void", OperationID: orderID})
}
