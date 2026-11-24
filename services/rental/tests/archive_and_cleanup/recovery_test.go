//go:build functional

package archive_and_cleanup

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkDetachRecovery(t *testing.T, binary string) {
	t.Helper()
	s := &archiveStand{tests.NewStand(t, binary)}
	client := &http.Client{Timeout: 4 * time.Second}
	api := s.StartRental(t, "api", "SOURCE_URL="+tests.NewSourceStub(t).URL).URL
	var now, week time.Time
	err := s.DB.QueryRow(t.Context(),
		"SELECT clock_timestamp(), date_trunc('week', clock_timestamp(), 'UTC')").Scan(&now, &week)
	if err != nil {
		t.Fatal(err)
	}
	week = week.UTC()
	previousWeek := week.AddDate(0, 0, -7)
	weekEnd := week.AddDate(0, 0, 7)
	past := week.Add(now.Sub(week) / 2)
	future := now.Add(weekEnd.Sub(now) / 2)
	expiredPartition := s.seedHistoryPartition(t, previousWeek)
	currentPartition := s.seedHistoryPartition(t, week)
	fullyExpired := s.seedHistoryTrip(t, previousWeek.Add(time.Hour))
	partiallyExpired := s.seedHistoryTrip(t, past)
	liveHistory := s.seedHistoryTrip(t, future)
	moving := s.seedTripToArchive(t, future)

	locker, err := s.DB.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	var oldRows int
	err = locker.QueryRow(t.Context(), "SELECT count(*) FROM orders_history WHERE delete_after >= $1 AND delete_after < $2",
		previousWeek, week).Scan(&oldRows)
	if err != nil || oldRows != 1 {
		t.Fatalf("данные раздела перед DETACH: %d, %v", oldRows, err)
	}

	var reads atomic.Int64
	readCtx, stopReading := context.WithCancel(t.Context())
	readDone := make(chan error, 1)
	go func() {
		var byOrder, byOffer []time.Duration
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for readCtx.Err() == nil {
			for _, trip := range []tripIDs{moving, liveHistory, partiallyExpired} {
				latency, err := readArchiveTrip(client, api, trip, 70)
				if err != nil {
					readDone <- err
					return
				}
				byOrder = append(byOrder, latency.byOrder)
				byOffer = append(byOffer, latency.byOffer)
			}
			reads.Add(1)
			select {
			case <-readCtx.Done():
			case <-ticker.C:
			}
		}
		if len(byOrder) != 0 {
			t.Logf("%d пар чтений до, во время и после DETACH. p95 по order_id %v, по offer_id %v",
				len(byOrder), p95(byOrder), p95(byOffer))
		}
		readDone <- nil
	}()
	defer func() {
		stopReading()
		if err := <-readDone; err != nil {
			t.Error(err)
		}
	}()
	tests.WaitFor(t, "чтений до старта maintenance", func() (bool, error) { return reads.Load() >= 3, nil })

	maintenanceEnv := []string{"MAINTENANCE_INTERVAL=100ms", "MAINTENANCE_LEASE=1s", "MAINTENANCE_PARTITION_WEEKS_AHEAD=1"}
	first := s.StartRental(t, "maintenance", maintenanceEnv...)
	tests.WaitFor(t, "незавершённого DETACH после архивации", func() (bool, error) {
		var pending, moved bool
		err := s.DB.QueryRow(t.Context(), `
			SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = 'orders_history'::regclass
				AND inhrelid = to_regclass($1) AND inhdetachpending),
				EXISTS (SELECT 1 FROM orders_history WHERE order_id = $2)`, expiredPartition, moving.order).
			Scan(&pending, &moved)
		return pending && moved, err
	})
	previousReads := reads.Load()
	tests.WaitFor(t, "параллельных чтений при незавершённом DETACH", func() (bool, error) { return reads.Load() >= previousReads+3, nil })

	var detachPID int
	tests.WaitFor(t, "сеанса DETACH, заблокированного тестовым читателем", func() (bool, error) {
		err := s.DB.QueryRow(t.Context(), `
			SELECT CASE WHEN count(*) = 1 THEN min(pid) ELSE 0 END
			FROM pg_stat_activity
			WHERE query LIKE 'ALTER TABLE orders_history DETACH PARTITION % CONCURRENTLY'
				AND $1 = ANY(pg_blocking_pids(pid))`, locker.Conn().PgConn().PID()).Scan(&detachPID)
		return detachPID != 0, err
	})
	first.Stop()
	// PostgreSQL может продолжать ожидание блокировки после гибели клиента.
	// Прерываем подтверждённый сеанс, сохраняя состояние незавершённого DETACH.
	var interrupted bool
	err = s.DB.QueryRow(t.Context(), `
		SELECT COALESCE((SELECT pg_terminate_backend(pid, 1000) FROM pg_stat_activity WHERE pid = $1), true)`,
		detachPID).Scan(&interrupted)
	if err != nil || !interrupted {
		t.Fatalf("прерывание сеанса DETACH %d: %t, %v", detachPID, interrupted, err)
	}
	var pending bool
	err = s.DB.QueryRow(t.Context(), `SELECT inhdetachpending FROM pg_inherits
		WHERE inhparent = 'orders_history'::regclass AND inhrelid = to_regclass($1)`, expiredPartition).Scan(&pending)
	if err != nil || !pending {
		t.Fatalf("после остановки ожидался незавершённый DETACH: pending=%t, %v", pending, err)
	}
	if err := locker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	next := s.seedTripToArchive(t, future)
	started := time.Now()
	second := s.StartRental(t, "maintenance", maintenanceEnv...)
	tests.WaitFor(t, "продолжения переноса и удаления раздела", func() (bool, error) {
		var dropped, partialSurvived, moved bool
		err := s.DB.QueryRow(t.Context(), `
			SELECT to_regclass($1) IS NULL, to_regclass($2) IS NOT NULL,
				(SELECT count(*) FROM orders_history WHERE order_id = $3) = 1`,
			expiredPartition, currentPartition, next.order).Scan(&dropped, &partialSurvived, &moved)
		return dropped && partialSurvived && moved, err
	})
	t.Logf("возобновление архивации и удаление раздела после рестарта заняли %v с учётом ожидания lease", time.Since(started))
	tests.WaitForMetric(t, client, second.URL, `rental_maintenance_rows_total{step="drop_partitions"} 1`)
	requireArchiveReads(t, client, api, next, 70)
	status, body, err := tests.RequestJSON[tests.OrderResponse](client, http.MethodGet, api+"/orders/"+fullyExpired.order, nil)
	if err != nil || status != http.StatusNotFound || body.ErrorCode != "NOT_FOUND" {
		t.Fatalf("заказ из удалённого раздела: HTTP %d, %+v, %v", status, body, err)
	}
	replay := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: fullyExpired.offer}, http.StatusNotFound)
	if replay.ErrorCode != "NOT_FOUND" {
		t.Fatalf("оффер из удалённого раздела: %+v", replay)
	}
	var complete bool
	err = s.DB.QueryRow(t.Context(), `
		SELECT NOT EXISTS (SELECT 1 FROM orders WHERE closed_at IS NOT NULL)
			AND (SELECT count(*) = 4 AND count(DISTINCT order_id) = 4
				FROM orders_history WHERE order_id IN ($1, $2, $3, $4))`,
		moving.order, next.order, liveHistory.order, partiallyExpired.order).Scan(&complete)
	if err != nil || !complete {
		t.Fatalf("после рестарта ожидаются четыре заказа в истории без дубликатов и пустая очередь переноса: %t, %v", complete, err)
	}
}
