//go:build functional

package zero_deposit_and_expired_start

import (
	"net/http"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestZeroDepositAndExpiredStart проверяет сценарий «Нулевой депозит и просроченный старт».
//
// Запускаем обработчик START до и после дедлайна заказа с нулевым депозитом
// и проверяем отсутствие вызовов Payment. Затем заменяем просроченный заказ
// с депозитом и проверяем, что worker завершает старый START и отправляет один VOID.
//
// План проверки — ../../../../docs/TEST_PLAN.md#нулевой-депозит-и-просроченный-старт
func TestZeroDepositAndExpiredStart(t *testing.T) {
	s := tests.NewStand(t, tests.BuildRental(t))
	source := tests.NewSourceStub(t)
	payment := tests.NewPaymentStub(t)
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL, "PAYMENT_URL="+payment.URL).URL
	client := &http.Client{Timeout: 4 * time.Second}

	checkZeroDeposit(t, s, client, api, source, payment)
	t.Run("Замена просроченного заказа с положительным депозитом", func(t *testing.T) {
		checkExpiredStartReplacement(t, s, client, api, source, payment)
	})
}
