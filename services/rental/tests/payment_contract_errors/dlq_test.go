//go:build functional

package payment_contract_errors

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkDLQContractError(t *testing.T, rentalBinary, paymentBinary, method string, response contractResponse) {
	s, reject := standWithContractError(t, rentalBinary, paymentBinary, method)
	var orderID string
	if method == "settle" {
		orderID = tests.StartTrip(t, s.Client, s.API, tests.OfferRequest{
			ScooterID: "scooter", UserID: "trusted-user", IdempotencyID: "settle-contract",
		}, 0)
	} else {
		orderID = s.StartOrder(t)
	}
	if method == "void" {
		s.ExpireStart(t, orderID)
	}
	worker := startContractErrorWorker(t, s, response)

	call := tests.PaymentCall{Method: method, OperationID: orderID}

	var finished tests.FinishResponse
	if method == "settle" {
		tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
		finished = s.FinishTrip(t, orderID, false)
	} else {
		tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	}
	kind := strings.ToUpper(method)
	dead := s.WaitPaymentTask(t, orderID, kind, "DEAD_LETTER")
	if dead.Attempts != 1 || dead.Claimed {
		t.Fatalf("ошибка контракта должна сразу отправить задачу в DLQ: %+v", dead)
	}
	requireContractError(t, dead.LastError, response)
	tests.WaitForMetric(t, s.Client, worker.URL, fmt.Sprintf(
		`rental_dependency_calls_total{dependency="payment",operation=%q,outcome="contract_error"} 1`, method))
	tests.WaitForMetric(t, s.Client, worker.URL, fmt.Sprintf(
		`rental_tasks_finished_total{kind=%q,reason="",status="DEAD_LETTER"} 1`, kind))
	waitForNextWorkerPoll(t, s, orderID, kind)
	if after := s.WaitPaymentTask(t, orderID, kind, "DEAD_LETTER"); after != dead {
		t.Fatalf("после следующего цикла worker задача DLQ изменилась: было %+v, стало %+v", dead, after)
	}
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), call)

	if method == "settle" {
		s.RepeatFinish(t, finished)
	} else {
		tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	}
	reject.Store(false)
	s.RentalCLI(t, "dlq", "requeue", dead.ID)
	done := s.WaitPaymentTask(t, orderID, kind, "COMPLETED")
	if done.ID != dead.ID || done.Parameters != dead.Parameters || done.Result != "success" || done.Attempts != 1 {
		t.Fatalf("задача после возврата: было %+v, стало %+v", dead, done)
	}
	attempts := s.Proxy.RecordedAttempts()
	tests.RequirePaymentCommands(t, attempts, call, call)
	if attempts[0].Body != attempts[1].Body {
		t.Fatalf("после возврата изменились параметры: %s и %s", attempts[0].Body, attempts[1].Body)
	}
	if method == "settle" {
		s.RequirePayment(t, orderID, http.StatusOK, "PAID")
		s.RepeatFinish(t, finished)
	} else {
		s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")
		tests.WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	}
}
