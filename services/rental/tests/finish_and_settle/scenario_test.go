//go:build functional

package finish_and_settle

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestFinishAndSettle проверяет сценарий «Завершение поездки и SETTLE».
//
// Завершаем поездки без HOLD, после его истечения и до пяти секунд при остановленном
// worker. Проверяем повтор finish, откат при ошибке создания SETTLE, долг при failed
// и возврат SETTLE и VOID из DLQ после длительного отказа Payment.
//
// План проверки — ../../../../docs/TEST_PLAN.md#завершение-поездки-и-settle
func TestFinishAndSettle(t *testing.T) {
	rentalBinary := tests.BuildRental(t)
	paymentBinary := tests.BuildPayment(t)
	t.Setenv("PAYMENT_SETTLE_RESULT", "success")

	checkFinishedTrips(t, rentalBinary, paymentBinary)
	t.Run("Ошибка вставки SETTLE откатывает finish", func(t *testing.T) {
		checkSettleInsertRollback(t, rentalBinary, paymentBinary)
	})
	t.Run("Failed у SETTLE создаёт DEBT", func(t *testing.T) {
		checkSettleDebt(t, rentalBinary, paymentBinary)
	})
	t.Run("Стоимость больше int64 сохраняется при SETTLE и архивации", func(t *testing.T) {
		checkLargeTripCost(t, rentalBinary, paymentBinary)
	})
	for _, payment := range []struct{ result, status string }{{"success", "PAID"}, {"failed", "DEBT"}} {
		t.Run("Потеря ответа SETTLE после "+payment.status, func(t *testing.T) {
			checkLostSettleResponse(t, rentalBinary, paymentBinary, payment.result, payment.status)
		})
	}
	checkPaymentRetriesAndRequeue(t, rentalBinary, paymentBinary)
}
