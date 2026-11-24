//go:build functional

package archive_and_cleanup

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestArchiveAndCleanup проверяет сценарий «Архивация и очистка».
//
// Закрываем поездки с разными сроками и состояниями задач, затем проверяем
// перенос и удаление. Измеряем обработку постоянного потока заказов и чтения истории,
// затем останавливаем maintenance при незавершённом DETACH и проверяем восстановление.
//
// План проверки — ../../../../docs/TEST_PLAN.md#архивация-и-очистка
func TestArchiveAndCleanup(t *testing.T) {
	binary := tests.BuildRental(t)

	t.Run("Сроки хранения и незавершённые задачи", func(t *testing.T) {
		checkRetentionAndTasks(t, binary)
	})
	t.Run("Перенос и очистка при постоянном потоке", func(t *testing.T) {
		checkArchiveUnderTraffic(t, binary)
	})
	t.Run("Чтение во время переноса и восстановление DETACH", func(t *testing.T) {
		checkDetachRecovery(t, binary)
	})
}
