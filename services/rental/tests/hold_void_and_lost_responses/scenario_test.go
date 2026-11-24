//go:build functional

package hold_void_and_lost_responses

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestHoldVoidAndLostResponses проверяет сценарий «HOLD, VOID и потерянные ответы».
//
// Останавливаем worker после успешного HOLD и отдельно теряем ответы HOLD и VOID.
// Задерживаем HOLD до или после VOID и убеждаемся, что поздний ответ не активирует заказ.
// При недоступном VOID проверяем истечение удержания в реальном Payment.
//
// План проверки — ../../../../docs/TEST_PLAN.md#hold-void-и-потерянные-ответы
func TestHoldVoidAndLostResponses(t *testing.T) {
	rentalBinary := tests.BuildRental(t)
	paymentBinary := tests.BuildPayment(t)

	checkHoldRecovery(t, rentalBinary, paymentBinary)
	t.Run("Явный отказ HOLD завершает START без VOID", func(t *testing.T) {
		checkHoldRejected(t, rentalBinary, paymentBinary)
	})
	checkVoidAndLateHold(t, rentalBinary, paymentBinary)
	t.Run("VOID недоступен до истечения удержания", func(t *testing.T) {
		checkHoldExpiresWithVoidUnavailable(t, rentalBinary, paymentBinary)
	})
}
