//go:build functional

package offers_and_concurrent_start

import (
	"net/http"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

// TestOffersAndConcurrentStart проверяет сценарий «Офферы и конкурентный старт».
//
// Повторяем создание оффера, меняем тариф и профиль, затем начинаем поездку
// на исходных условиях. Отдельно проверяем истёкший оффер и параллельный
// старт по нескольким офферам на один самокат и по одному офферу через две реплики API.
//
// План проверки — ../../../../docs/TEST_PLAN.md#офферы-и-конкурентный-старт
func TestOffersAndConcurrentStart(t *testing.T) {
	binary := tests.BuildRental(t)
	s := tests.NewStand(t, binary)
	source := tests.NewSourceStub(t)
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL).URL
	client := &http.Client{Timeout: 4 * time.Second}

	t.Run("Повтор оффера и сохранение цены", func(t *testing.T) {
		checkOfferReplay(t, s, client, api, source)
	})
	t.Run("Истёкший оффер", func(t *testing.T) {
		checkExpiredOffer(t, s, client, api)
	})
	t.Run("Два оффера на один самокат через две реплики", func(t *testing.T) {
		checkConcurrentStarts(t, binary, client, false)
	})
	t.Run("Одновременный старт по одному офферу через две реплики", func(t *testing.T) {
		checkConcurrentStarts(t, binary, client, true)
	})
	t.Run("Тариф за пределами диапазона", func(t *testing.T) {
		checkOfferRateOverflow(t, binary, client)
	})
}
