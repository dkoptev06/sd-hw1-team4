//go:build functional

package archive_and_cleanup

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkRetentionAndTasks(t *testing.T, binary string) {
	s := &archiveStand{tests.NewStand(t, binary)}
	client := &http.Client{Timeout: 4 * time.Second}

	const (
		validConfig = iota
		nullZoneConfig
		invalidConfig
		unavailableConfig
		updatedConfig
	)
	var configMode atomic.Int32
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.URL.Path == "/configs":
			switch configMode.Load() {
			case validConfig:
				tests.WriteSourceConfig(w, 1.25, map[string]int{"zone-2": 30}, 10)
			case nullZoneConfig:
				tests.WriteSourceConfig(w, 2, nil, 1)
			case invalidConfig:
				tests.WriteSourceConfig(w, 1.25, map[string]int{"zone-2": -1}, 10)
			case unavailableConfig:
				http.Error(w, "configs unavailable", http.StatusServiceUnavailable)
			case updatedConfig:
				tests.WriteSourceConfig(w, 1.5, map[string]int{"zone-2": 60}, 10)
			}
		case r.URL.Path == "/user-profile/fallback":
			http.Error(w, "profile unavailable", http.StatusServiceUnavailable)
		case strings.HasPrefix(r.URL.Path, "/scooter/zone-2-"):
			tests.WriteJSON(w, map[string]any{"id": strings.TrimPrefix(r.URL.Path, "/scooter/"), "zone_id": "zone-2", "charge": 80})
		default:
			return false
		}
		return true
	})
	payment := tests.NewPaymentStub(t)
	var rejectCommands atomic.Bool
	proxy := tests.NewPaymentProxy(t, payment.URL, tests.PaymentFaults{
		ResponseStatus: func(path string) int {
			if path == "/payments/void" || path == "/payments/settle" && rejectCommands.Load() {
				return http.StatusConflict
			}
			return 0
		},
	})
	const refresh = "CONFIG_REFRESH_INTERVAL=100ms"
	api := s.StartRental(t, "api", "SOURCE_URL="+source.URL, refresh).URL

	starting := createArchiveTrip(t, client, api, "starting", "trusted-user")
	s.execFixture(t, `UPDATE tasks SET next_run_at = clock_timestamp() + interval '1 day' WHERE order_id = $1`, starting.order)
	voidDLQ := createArchiveTrip(t, client, api, "zone-2-void", "user-1")
	s.ExpireStart(t, voidDLQ.order)
	s.StartRental(t, "worker", "SOURCE_URL="+source.URL, "PAYMENT_URL="+proxy.URL,
		"WORKER_POLL_INTERVAL=20ms", "WORKER_SWEEP_INTERVAL=20ms", refresh)

	s.WaitPaymentTask(t, voidDLQ.order, "VOID", "DEAD_LETTER")
	if start := s.ReadStart(t, voidDLQ.order); start.Status != "COMPLETED" || start.Reason != "DEADLINE_EXCEEDED" {
		t.Fatalf("просроченный START: %+v", start)
	}
	s.requireRetention(t, voidDLQ.order, 30)
	active := createArchiveTrip(t, client, api, "active", "trusted-user")
	tests.WaitForOrder(t, client, api, active.order, "ACTIVE")

	configMode.Store(nullZoneConfig)
	const invalidRefresh = `rental_config_refreshes_total{result="invalid"}`
	tests.WaitFor(t, "отклонения config с retention_days_by_zone: null", func() (bool, error) {
		n, err := tests.MetricValue(client, api, invalidRefresh)
		return n >= 1, err
	})
	shortRetention := createArchiveTrip(t, client, api, "zone-2-fallback", "fallback")
	shortFinish := s.finishForArchive(t, client, api, shortRetention, "COMPLETED")
	if err := s.DB.QueryRow(t.Context(), `SELECT started_at, finish_time FROM orders WHERE order_id=$1`, shortRetention.order).Scan(&shortRetention.started, &shortRetention.finished); err != nil {
		t.Fatal(err)
	}
	shortDeleteAfter := s.requireRetention(t, shortRetention.order, 30)
	longRetention := createArchiveTrip(t, client, api, "default", "trusted-user")
	s.finishForArchive(t, client, api, longRetention, "COMPLETED")
	s.requireRetention(t, longRetention.order, 365)

	configMode.Store(updatedConfig)
	var updatedOffer tests.OfferResponse
	attempt := 0
	tests.WaitFor(t, "применения config с surge 1.5 и сроком 60 дней", func() (bool, error) {
		attempt++
		updatedOffer = tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
			ScooterID: "zone-2-updated", UserID: "trusted-user", IdempotencyID: fmt.Sprintf("updated-config-%d", attempt),
		}, http.StatusCreated)
		return updatedOffer.MinuteRate == 18, nil
	})
	changed := startArchiveTrip(t, client, api, updatedOffer.OfferID)
	s.finishForArchive(t, client, api, changed, "COMPLETED")
	s.requireRetention(t, changed.order, 60)

	pending := s.seedTripToArchive(t, time.Now().Add(-24*time.Hour))
	s.execFixture(t, `
		INSERT INTO tasks (task_id, order_id, task_type, parameters, created_at, next_run_at)
		VALUES (gen_random_uuid(), $1, 'SETTLE', '{}', clock_timestamp(), clock_timestamp() + interval '1 day')`,
		pending.order)

	const firstCycleFinished = `rental_maintenance_step_duration_seconds_count{step="drop_partitions"} 1`
	maintenanceEnv := []string{"MAINTENANCE_INTERVAL=1h", "MAINTENANCE_LEASE=1s",
		"MAINTENANCE_PARTITION_WEEKS_AHEAD=1", "MAINTENANCE_BATCH_SIZE=2"}
	maintenance := s.StartRental(t, "maintenance", maintenanceEnv...)
	tests.WaitForMetric(t, client, maintenance.URL, firstCycleFinished)
	var moved, completedTasks int
	err := s.DB.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM orders_history WHERE order_id IN ($1, $2, $3)),
			(SELECT count(*) FROM tasks WHERE status = 'COMPLETED')`,
		shortRetention.order, longRetention.order, changed.order).Scan(&moved, &completedTasks)
	if err != nil || moved != 3 || completedTasks != 0 {
		t.Fatalf("за первый цикл ожидались 3 переноса после очистки задач: перенесено %d, осталось задач COMPLETED %d, %v",
			moved, completedTasks, err)
	}
	requireArchiveReads(t, client, api, shortRetention, *shortFinish.Cost)
	maintenance.Stop()

	previousInvalid, err := tests.MetricValue(client, api, invalidRefresh)
	if err != nil {
		t.Fatal(err)
	}
	configMode.Store(invalidConfig)
	tests.WaitFor(t, "невалидного обновления config", func() (bool, error) {
		n, err := tests.MetricValue(client, api, invalidRefresh)
		return n > previousInvalid, err
	})
	invalid := createArchiveTrip(t, client, api, "zone-2-invalid", "trusted-user")
	s.finishForArchive(t, client, api, invalid, "COMPLETED")
	s.requireRetention(t, invalid.order, 60)

	configMode.Store(unavailableConfig)
	const unavailableRefresh = `rental_config_refreshes_total{result="unavailable"}`
	tests.WaitFor(t, "недоступного обновления config", func() (bool, error) {
		n, err := tests.MetricValue(client, api, unavailableRefresh)
		return n >= 1, err
	})
	warm := createArchiveTrip(t, client, api, "zone-2-warm", "trusted-user")
	s.finishForArchive(t, client, api, warm, "COMPLETED")
	s.requireRetention(t, warm.order, 60)
	coldAPI := s.StartRental(t, "api", "SOURCE_URL="+source.URL, refresh).URL
	tests.WaitForMetric(t, client, coldAPI, "rental_config_using_defaults 1")
	expired := createArchiveTrip(t, client, coldAPI, "zone-2-cold", "trusted-user")
	s.finishForArchive(t, client, coldAPI, expired, "COMPLETED")
	s.requireRetention(t, expired.order, 365)

	rejectCommands.Store(true)
	settleDLQ := createArchiveTrip(t, client, api, "settle-dlq", "trusted-user")
	s.finishForArchive(t, client, api, settleDLQ, "DEAD_LETTER")
	for _, trip := range []tripIDs{expired, settleDLQ, voidDLQ} {
		s.execFixture(t, `UPDATE orders SET delete_after = clock_timestamp() - interval '1 day' WHERE order_id = $1`, trip.order)
	}

	tests.WaitFor(t, "истечения lease первого maintenance", func() (bool, error) {
		var available bool
		err := s.DB.QueryRow(t.Context(), `
			SELECT NOT EXISTS (SELECT 1 FROM maintenance_leases WHERE lease_until > clock_timestamp())`).Scan(&available)
		return available, err
	})
	maintenance = s.StartRental(t, "maintenance", maintenanceEnv...)
	tests.WaitForMetric(t, client, maintenance.URL, firstCycleFinished)
	var archived, deleted bool
	err = s.DB.QueryRow(t.Context(), `
		SELECT (SELECT count(*) FROM orders_history WHERE order_id IN ($1, $2)) = 2,
			NOT EXISTS (SELECT 1 FROM orders WHERE order_id = $3)`,
		invalid.order, warm.order, expired.order).Scan(&archived, &deleted)
	if err != nil || !archived || !deleted {
		t.Fatalf("первый цикл при недоступном config: неистёкшие заказы перенесены=%t, истёкший удалён=%t, %v",
			archived, deleted, err)
	}
	var copied, removedCurrent, removedOffer, removedTasks, skippedHistory bool
	err = s.DB.QueryRow(t.Context(), `
		SELECT (SELECT user_profile_fallback_used AND delete_after = $5 FROM orders_history WHERE order_id = $1),
			NOT EXISTS (SELECT 1 FROM orders WHERE order_id IN ($1, $3, $4)),
			NOT EXISTS (SELECT 1 FROM offers WHERE offer_id IN ($2, $6)),
			NOT EXISTS (SELECT 1 FROM tasks WHERE status = 'COMPLETED'),
			NOT EXISTS (SELECT 1 FROM orders_history WHERE order_id = $4)`,
		shortRetention.order, shortRetention.offer, longRetention.order, expired.order, shortDeleteAfter, expired.offer).
		Scan(&copied, &removedCurrent, &removedOffer, &removedTasks, &skippedHistory)
	if err != nil || !copied || !removedCurrent || !removedOffer || !removedTasks || !skippedHistory {
		t.Fatalf("архивация: поля=%t, orders=%t, offers=%t, tasks=%t, истёкший заказ не перенесён=%t, %v",
			copied, removedCurrent, removedOffer, removedTasks, skippedHistory, err)
	}
	s.WaitPaymentTask(t, settleDLQ.order, "SETTLE", "DEAD_LETTER")
	s.WaitPaymentTask(t, voidDLQ.order, "VOID", "DEAD_LETTER")
	tests.WaitForOrder(t, client, api, active.order, "ACTIVE")
	tests.WaitForOrder(t, client, api, starting.order, "STARTING")
	tests.WaitForOrder(t, client, api, voidDLQ.order, "START_FAILED")
	tests.WaitForOrder(t, client, api, settleDLQ.order, "COMPLETED")
	s.WaitPaymentTask(t, pending.order, "SETTLE", "PENDING")
	tests.WaitForOrder(t, client, api, pending.order, "COMPLETED")
	tests.WaitForMetric(t, client, maintenance.URL, `rental_maintenance_rows_total{step="archive_orders_moved"} 2`)
	t.Logf("Сохранённые сроки 30, 60 и 365 дней. При невалидном и недоступном config тёплый API сохранил 60 дней, холодный применил 365. Перенесено пять заказов, истёкший заказ удалён, PENDING и DLQ оставлены в orders")
}
