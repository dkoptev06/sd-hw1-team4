//go:build functional

package archive_and_cleanup

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkArchiveUnderTraffic(t *testing.T, binary string) {
	s := &archiveStand{tests.NewStand(t, binary)}
	client := &http.Client{Timeout: 4 * time.Second}

	const (
		historyWeeks        = 52
		ordersPerWeek       = 100
		expiredOrders       = 256
		maintenanceInterval = 100 * time.Millisecond
		writeInterval       = 40 * time.Millisecond
		ordersPerWrite      = 4
		writesPerWindow     = 40
		steadyWindows       = 3
		windowDuration      = writesPerWindow * writeInterval
		totalWrites         = (1 + steadyWindows) * writesPerWindow
	)
	var thisWeek time.Time
	err := s.DB.QueryRow(t.Context(), "SELECT date_trunc('week', clock_timestamp(), 'UTC')").Scan(&thisWeek)
	if err != nil {
		t.Fatal(err)
	}
	thisWeek = thisWeek.UTC()
	s.seedHistoryPartition(t, thisWeek)
	expiredWeek := thisWeek.AddDate(0, 0, -7)
	expiredPartition := s.seedHistoryPartition(t, expiredWeek)
	s.seedHistoryTrips(t, expiredWeek.Add(time.Hour), expiredOrders)

	history := make([]tripIDs, 0, historyWeeks*ordersPerWeek)
	for i := 1; i <= historyWeeks; i++ {
		week := thisWeek.AddDate(0, 0, i*7)
		s.seedHistoryPartition(t, week)
		history = append(history, s.seedHistoryTrips(t, week.Add(time.Hour), ordersPerWeek)...)
	}
	// Срок новых записей отличается от подготовленной истории, чтобы измерять
	// их очередь и завершившиеся переносы при каждом замере.
	archiveDeadline := thisWeek.AddDate(0, 0, 7).Add(2 * time.Hour)

	api := s.StartRental(t, "api", "SOURCE_URL="+tests.NewSourceStub(t).URL).URL
	var readCount atomic.Int64
	readCtx, stopReading := context.WithCancel(t.Context())
	readDone := make(chan historyReadReport, 1)
	go func() { readDone <- readHistory(readCtx, client, api, history, &readCount) }()
	defer func() {
		stopReading()
		report := <-readDone
		if report.err != nil {
			t.Error(report.err)
		}
		if len(report.byOrder) != 0 {
			t.Logf("%d заказов в %d недельных разделах, %d пар запросов по истории: p95 по order_id %v, по offer_id %v",
				len(history), historyWeeks, len(report.byOrder), p95(report.byOrder), p95(report.byOffer))
		}
	}()
	tests.WaitFor(t, "чтений до запуска обслуживания", func() (bool, error) { return readCount.Load() >= 3, nil })

	locker, err := s.DB.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(context.Background())
	var oldRows int
	if err := locker.QueryRow(t.Context(), `SELECT count(*) FROM orders_history WHERE delete_after >= $1 AND delete_after < $2`,
		expiredWeek, thisWeek).Scan(&oldRows); err != nil || oldRows != expiredOrders {
		t.Fatalf("до очистки в разделе ожидалось %d строк, найдено %d, ошибка %v", expiredOrders, oldRows, err)
	}
	maintenance := s.StartRental(t, "maintenance", "MAINTENANCE_INTERVAL="+maintenanceInterval.String(),
		"MAINTENANCE_PARTITION_WEEKS_AHEAD=1", "MAINTENANCE_BATCH_SIZE=16")
	tests.WaitFor(t, "DETACH, ожидающего читателя", func() (bool, error) {
		var pending bool
		err := s.DB.QueryRow(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_inherits WHERE inhparent = 'orders_history'::regclass
			AND inhrelid = to_regclass($1) AND inhdetachpending)`, expiredPartition).Scan(&pending)
		return pending, err
	})

	// The independent writer follows absolute deadlines. Check committed writes in
	// fixed wall-clock windows so database or observation delays cannot lower the
	// offered rate and silently stretch the measurement.
	trafficStarted := time.Now()
	writeCtx, stopWriting := context.WithCancel(t.Context())
	var batches []observedArchiveBatch
	var writeErr error
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := range totalWrites {
			due := trafficStarted.Add(time.Duration(i) * writeInterval)
			timer := time.NewTimer(time.Until(due))
			select {
			case <-writeCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			submitted := time.Now()
			trips, err := s.seedTripsToArchive(writeCtx, archiveDeadline, ordersPerWrite, true)
			if err != nil || len(trips) != ordersPerWrite {
				writeErr = fmt.Errorf("фикстура переноса: ожидалось %d заказов, создано %d, ошибка %v", ordersPerWrite, len(trips), err)
				return
			}
			batches = append(batches, observedArchiveBatch{submittedAt: submitted, committedAt: time.Now()})
		}
	}()
	defer func() { stopWriting(); <-writerDone }()

	var releasedAt time.Time
	var deletionObservedAfter time.Duration
	var archivedWhilePending int
	observeTransfer := func() (archiveProgress, error) {
		sample := archiveProgress{}
		var lagSeconds float64
		err := s.DB.QueryRow(t.Context(), `
            SELECT count(*), coalesce(extract(epoch FROM statement_timestamp()-min(closed_at)),0)::float8,
                (SELECT count(*) FROM orders_history WHERE delete_after=$1),to_regclass($2) IS NULL
            FROM orders WHERE delete_after=$1`, archiveDeadline, expiredPartition).
			Scan(&sample.pending, &lagSeconds, &sample.archived, &sample.dropped)
		sample.oldest = time.Duration(lagSeconds * float64(time.Second))
		sample.at = time.Now()
		return sample, err
	}
	var samples []archiveProgress
	var windows [1 + steadyWindows]archiveProgress
	readsBefore := readCount.Load()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for i := range windows {
		deadline := trafficStarted.Add(time.Duration(i+1) * windowDuration)
		for {
			<-ticker.C
			progress, err := observeTransfer()
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, progress)
			if releasedAt.IsZero() && progress.at.Sub(trafficStarted) >= 3*maintenanceInterval && progress.archived > 0 && readCount.Load() > readsBefore {
				archivedWhilePending = progress.archived
				releasedAt = time.Now()
				if err := locker.Rollback(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if progress.dropped && deletionObservedAfter == 0 {
				if releasedAt.IsZero() {
					t.Fatal("раздел удалён при удерживаемом читателе")
				}
				deletionObservedAfter = progress.at.Sub(releasedAt)
			}
			windows[i].archived = progress.archived
			windows[i].pending = max(windows[i].pending, progress.pending)
			windows[i].oldest = max(windows[i].oldest, progress.oldest)
			if !progress.at.Before(deadline) {
				break
			}
		}
		if i == 0 && archivedWhilePending == 0 {
			t.Fatal("во время ожидания DETACH свободные заказы не переносились в историю")
		}
		archivedBefore := 0
		if i > 0 {
			archivedBefore = windows[i-1].archived
		}
		reads := readCount.Load()
		transferred := windows[i].archived - archivedBefore
		t.Logf("Окно %d (%v): перенесено %d, пар чтений %d, пик очереди %d, максимальный возраст %v", i+1, windowDuration, transferred, reads-readsBefore, windows[i].pending, windows[i].oldest)
		if transferred == 0 || reads == readsBefore {
			t.Fatalf("чтения или перенос остановились в окне %d", i+1)
		}
		readsBefore = reads
	}
	if deletionObservedAfter <= 0 || releasedAt.Add(deletionObservedAfter).After(trafficStarted.Add(time.Duration(totalWrites-1)*writeInterval)) {
		t.Fatal("физическое удаление раздела ожидалось при действующей нагрузке")
	}
	<-writerDone
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if len(batches) != totalWrites {
		t.Fatalf("записано %d из %d пакетов", len(batches), totalWrites)
	}
	var committed [1 + steadyWindows]int
	for _, batch := range batches {
		index := int(batch.committedAt.Sub(trafficStarted) / windowDuration)
		if index < len(committed) {
			committed[index] += ordersPerWrite
		}
	}
	for i, n := range committed {
		const target = ordersPerWrite * writesPerWindow
		if n < target-target/10 || n > target+target/10 {
			t.Fatalf("в окне %d за %v записано %d заказов, ожидалось %d ±10%%", i+1, windowDuration, n, target)
		}
		t.Logf("Окно %d: записано %d, %.1f заказов/с", i+1, n, float64(n)/windowDuration.Seconds())
	}
	const written = totalWrites * ordersPerWrite

	const lagTolerance = 2 * maintenanceInterval
	const pendingTolerance = int(lagTolerance/writeInterval) * ordersPerWrite
	for i := 2; i < len(windows); i++ {
		if windows[i].pending > windows[1].pending+pendingTolerance || windows[i].oldest > windows[1].oldest+lagTolerance {
			t.Fatalf("растёт отставание переноса после удаления раздела: первое стабильное окно %+v, окно %d %+v",
				windows[1], i+1, windows[i])
		}
	}
	tests.WaitForMetric(t, client, maintenance.URL, `rental_maintenance_rows_total{step="drop_partitions"} 1`)
	t.Logf("За время ожидания DETACH перенесено %d заказов", archivedWhilePending)
	t.Logf("Удалён раздел с %d заказами, обнаружено через %v после снятия блокировки читателя, %0.1f заказов/с за измеренный интервал",
		expiredOrders, deletionObservedAfter, float64(expiredOrders)/deletionObservedAfter.Seconds())

	tests.WaitFor(t, "завершения переносов после остановки записей", func() (bool, error) {
		progress, err := observeTransfer()
		samples = append(samples, progress)
		return progress.pending == 0 && progress.archived == written, err
	})
	var transferLatencies []time.Duration
	for _, progress := range samples {
		for len(transferLatencies) < len(batches) && (len(transferLatencies)+1)*ordersPerWrite <= progress.archived {
			b := batches[len(transferLatencies)]
			transferLatencies = append(transferLatencies, progress.at.Sub(b.submittedAt))
		}
	}
	var distinct, remainingTasks int
	err = s.DB.QueryRow(t.Context(), `
		SELECT count(DISTINCT order_id), (SELECT count(*) FROM tasks WHERE status = 'COMPLETED')
		FROM orders_history WHERE delete_after = $1`, archiveDeadline).Scan(&distinct, &remainingTasks)
	if err != nil || distinct != written || remainingTasks != 0 || len(transferLatencies) != writesPerWindow*len(windows) {
		t.Fatalf("полнота архива: уникальных %d из %d, оставшиеся задачи COMPLETED %d, замеров %d, %v",
			distinct, written, remainingTasks, len(transferLatencies), err)
	}
	t.Logf("p95 видимости перенесённых пакетов в истории %v по %d замерам", p95(transferLatencies), len(transferLatencies))
}

type observedArchiveBatch struct {
	submittedAt, committedAt time.Time
}

type archiveProgress struct {
	dropped  bool
	at       time.Time
	pending  int
	archived int
	oldest   time.Duration
}

type historyReadReport struct {
	byOrder []time.Duration
	byOffer []time.Duration
	err     error
}

func readHistory(ctx context.Context, client *http.Client, api string, trips []tripIDs, count *atomic.Int64) historyReadReport {
	var report historyReadReport
	random := rand.New(rand.NewPCG(4, 1))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		var latency archiveReadLatency
		latency, report.err = readArchiveTrip(client, api, trips[random.IntN(len(trips))], 70)
		if report.err != nil {
			break
		}
		report.byOrder = append(report.byOrder, latency.byOrder)
		report.byOffer = append(report.byOffer, latency.byOffer)
		count.Add(1)
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	return report
}

func p95(samples []time.Duration) time.Duration {
	slices.Sort(samples)
	return samples[(len(samples)*95-1)/100]
}
