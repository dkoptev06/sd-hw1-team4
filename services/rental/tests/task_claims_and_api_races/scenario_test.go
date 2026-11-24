//go:build functional

package task_claims_and_api_races

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestTaskClaimsAndAPIRaces проверяет сценарий «Захват задач и гонки с API».
//
// Проверяем успешную последнюю попытку и падение worker с истечением lease.
// Задерживаем ответ HOLD, пока другой worker закрывает START по дедлайну
// или перехватывает ещё не просроченную задачу после истечения lease.
// Повторяем гонку замены заказа через API с успехом, ошибкой и падением worker.
// Отказ записи COMPLETED проверяет откат заказа и VOID в той же транзакции.
//
// План проверки — ../../../../docs/TEST_PLAN.md#захват-задач-и-гонки-с-api
func TestTaskClaimsAndAPIRaces(t *testing.T) {
	rentalBinary := tests.BuildRental(t)
	paymentBinary := tests.BuildPayment(t)
	lastAttempt := []string{"START_MAX_ATTEMPTS=1", "PAYMENT_TIMEOUT=2s", "WORKER_LEASE=3s"}

	checkFinalAttempt(t, rentalBinary, paymentBinary, lastAttempt)
	t.Run("Дедлайн и lease истекли при отложенном next_run_at", func(t *testing.T) {
		checkLateHold(t, rentalBinary, paymentBinary, true)
	})
	t.Run("Дедлайн истёк при действующем lease и отложенном next_run_at", func(t *testing.T) {
		checkLateHold(t, rentalBinary, paymentBinary, false)
	})
	t.Run("Прежний worker не может записать ответ при новом действующем захвате", func(t *testing.T) {
		checkReclaimedStart(t, rentalBinary, paymentBinary)
	})
	t.Run("Заблокированный заказ не задерживает независимый просроченный START", func(t *testing.T) {
		checkBlockedOrder(t, rentalBinary, paymentBinary)
	})
	checkReplacementDuringHold(t, rentalBinary, paymentBinary)
	t.Run("Отказ записи COMPLETED откатывает закрытие и VOID", func(t *testing.T) {
		checkStartCompletionRollback(t, rentalBinary, paymentBinary)
	})
}
