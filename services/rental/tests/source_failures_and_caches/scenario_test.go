//go:build functional

package source_failures_and_caches

import (
	"net/http"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestSourceFailuresAndCaches проверяет сценарий «Отказы Source и работа кэшей».
//
// Вызываем Rental при сбоях scooters, users, tariff-zones и configs. Проверяем
// актуальность данных для старта, цены при резервном профиле, истечение TTL и вытеснение зон
// и сохранение последней валидной конфигурации и условий выданного оффера.
//
// План проверки — ../../../../docs/TEST_PLAN.md#отказы-source-и-работа-кэшей
func TestSourceFailuresAndCaches(t *testing.T) {
	binary := tests.BuildRental(t)
	client := &http.Client{Timeout: 4 * time.Second}

	t.Run("Свежесть самоката при оффере и старте", func(t *testing.T) {
		checkScooterFreshness(t, binary, client)
	})
	t.Run("Резервный профиль и ошибки Source", func(t *testing.T) {
		checkProfileFallback(t, binary, client)
	})
	t.Run("Последнее валидное значение config", func(t *testing.T) {
		checkConfigRefresh(t, binary, client)
	})
	t.Run("Вытеснение зон по LRU", func(t *testing.T) {
		checkTariffCacheEviction(t, binary, client)
	})
	t.Run("Просроченный кэш зоны", func(t *testing.T) {
		checkTariffExpiration(t, binary, client)
	})
}
