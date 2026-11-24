//go:build functional

package payment_contract_errors

import (
	"strings"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestPaymentContractErrors проверяет сценарий «Неожиданные 4xx от Payment».
//
// Отвечаем ошибкой контракта на HOLD, SETTLE и VOID с полным или оборванным телом.
// Проверяем закрытие START с компенсацией, отправку в DLQ после одной попытки,
// отсутствие повторного захвата и сохранение параметров после ручного возврата.
//
// План проверки — ../../../../docs/TEST_PLAN.md#неожиданные-4xx-от-payment
func TestPaymentContractErrors(t *testing.T) {
	rentalBinary := tests.BuildRental(t)
	paymentBinary := tests.BuildPayment(t)
	t.Setenv("PAYMENT_SETTLE_RESULT", "success")

	for _, response := range []contractResponse{
		{errorPrefix: "status 409:"},
		{suffix: " с оборванным телом 409", modify: incompleteConflict, errorPrefix: "status 409: read body: unexpected EOF"},
		{suffix: " с длинным текстом 409", modify: localizedConflict, errorPrefix: "status 409: заказ"},
	} {
		t.Run("HOLD"+response.suffix, func(t *testing.T) {
			checkHoldContractError(t, rentalBinary, paymentBinary, response)
		})
		for _, method := range []string{"settle", "void"} {
			t.Run(strings.ToUpper(method)+response.suffix, func(t *testing.T) {
				checkDLQContractError(t, rentalBinary, paymentBinary, method, response)
			})
		}
	}
}
