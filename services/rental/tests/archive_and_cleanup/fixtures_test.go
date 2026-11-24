//go:build functional

package archive_and_cleanup

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

type archiveStand struct {
	*tests.Stand
}

type tripIDs struct {
	order             string
	offer             string
	started, finished time.Time
}

func createArchiveTrip(t *testing.T, client *http.Client, api, scooter, user string) tripIDs {
	t.Helper()
	offer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
		ScooterID: scooter, UserID: user, IdempotencyID: scooter,
	}, http.StatusCreated)
	return startArchiveTrip(t, client, api, offer.OfferID)
}

func startArchiveTrip(t *testing.T, client *http.Client, api, offerID string) tripIDs {
	t.Helper()
	start := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: offerID}, http.StatusAccepted)
	if start.Status != "STARTING" || start.OrderID == "" {
		t.Fatalf("создание поездки: %+v", start)
	}
	return tripIDs{order: start.OrderID, offer: offerID}
}

func (s *archiveStand) finishForArchive(t *testing.T, client *http.Client, api string, trip tripIDs, taskStatus string) tests.FinishResponse {
	t.Helper()
	tests.WaitForOrder(t, client, api, trip.order, "ACTIVE")
	s.execFixture(t, `UPDATE orders SET started_at = clock_timestamp() - interval '1 minute' WHERE order_id = $1`, trip.order)
	finish := tests.RequirePost[tests.FinishResponse](t, client, api+"/orders/"+trip.order+"/finish", nil, http.StatusOK)
	if finish.Status != "COMPLETED" || finish.Cost == nil {
		t.Fatalf("завершение поездки: %+v", finish)
	}
	s.WaitPaymentTask(t, trip.order, "SETTLE", taskStatus)
	return finish
}

func (s *archiveStand) requireRetention(t *testing.T, id string, days int) time.Time {
	t.Helper()
	var closed, deleteAfter time.Time
	err := s.DB.QueryRow(t.Context(), `SELECT closed_at, delete_after FROM orders WHERE order_id = $1`, id).
		Scan(&closed, &deleteAfter)
	if err != nil || !deleteAfter.Equal(closed.UTC().AddDate(0, 0, days)) {
		t.Fatalf("заказ %s: срок должен быть %d дней от %v, получено %v, %v", id, days, closed, deleteAfter, err)
	}
	return deleteAfter
}

func (s *archiveStand) execFixture(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := s.DB.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func requireArchiveReads(t *testing.T, client *http.Client, api string, trip tripIDs, cost int64) {
	t.Helper()
	if _, err := readArchiveTrip(client, api, trip, cost); err != nil {
		t.Fatal(err)
	}
}

type archiveReadLatency struct {
	byOrder time.Duration
	byOffer time.Duration
}

func readArchiveTrip(client *http.Client, api string, trip tripIDs, cost int64) (archiveReadLatency, error) {
	var latency archiveReadLatency
	started := time.Now()
	status, current, err := tests.RequestJSON[tests.OrderResponse](client, http.MethodGet, api+"/orders/"+trip.order, nil)
	latency.byOrder = time.Since(started)
	if err != nil || status != http.StatusOK || current.OrderID != trip.order || current.Status != "COMPLETED" ||
		current.StartedAt == nil || current.FinishTime == nil ||
		(!trip.started.IsZero() && !current.StartedAt.Equal(trip.started)) ||
		(!trip.finished.IsZero() && !current.FinishTime.Equal(trip.finished)) || current.Cost == nil || *current.Cost != cost {
		return latency, fmt.Errorf("GET %s: HTTP %d, %+v, %v", trip.order, status, current, err)
	}
	started = time.Now()
	status, replay, err := tests.RequestJSON[tests.StartResponse](client, http.MethodPost, api+"/orders", tests.StartRequest{OfferID: trip.offer})
	latency.byOffer = time.Since(started)
	if err != nil || status != http.StatusOK || replay.OrderID != trip.order || replay.Status != "COMPLETED" {
		return latency, fmt.Errorf("повтор старта %s: HTTP %d, %+v, %v", trip.offer, status, replay, err)
	}
	return latency, nil
}

func (s *archiveStand) seedHistoryPartition(t *testing.T, start time.Time) string {
	t.Helper()
	name := "orders_history_p" + start.UTC().Format("20060102")
	s.execFixture(t, fmt.Sprintf("CREATE TABLE %s PARTITION OF orders_history FOR VALUES FROM ('%s') TO ('%s')",
		pgx.Identifier{name}.Sanitize(), start.Format(time.RFC3339), start.AddDate(0, 0, 7).Format(time.RFC3339)))
	return name
}

func (s *archiveStand) seedHistoryTrip(t *testing.T, deleteAfter time.Time) tripIDs {
	t.Helper()
	return s.seedHistoryTrips(t, deleteAfter, 1)[0]
}

func (s *archiveStand) seedHistoryTrips(t *testing.T, deleteAfter time.Time, count int) []tripIDs {
	t.Helper()
	rows, err := s.DB.Query(t.Context(), `
		INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate,
			unlock_fee, deposit, status, created_at, started_at, closed_at, finish_time, delete_after, cost, user_profile_fallback_used)
		SELECT gen_random_uuid(), gen_random_uuid(), 'user', 'scooter', 'zone-1', 15,
			40, 0, 'COMPLETED', $1::timestamptz - interval '365 days 3 minutes', $1::timestamptz - interval '365 days 2 minutes', $1::timestamptz - interval '365 days',
			$1::timestamptz - interval '365 days', $1, 70, false FROM generate_series(1, $2::integer)
		RETURNING order_id::text, offer_id::text, started_at, finish_time`, deleteAfter, count)
	if err != nil {
		t.Fatal(err)
	}
	trips, err := pgx.CollectRows(rows, scanTripIDs)
	if err != nil {
		t.Fatal(err)
	}
	return trips
}

func (s *archiveStand) seedTripToArchive(t *testing.T, deleteAfter time.Time) tripIDs {
	t.Helper()
	trips, err := s.seedTripsToArchive(t.Context(), deleteAfter, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	return trips[0]
}

func (s *archiveStand) seedTripsToArchive(ctx context.Context, deleteAfter time.Time, count int, withCompletedTasks bool) ([]tripIDs, error) {
	rows, err := s.DB.Query(ctx, `
		WITH offer AS (
			INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge, minute_rate,
				unlock_fee, deposit, user_profile_fallback_used, status, created_at, expires_at)
			SELECT gen_random_uuid(), gen_random_uuid()::text, 'user', 'scooter', 'zone-1', 80, 15, 40, 0, false,
				'CONSUMED', statement_timestamp(), statement_timestamp() FROM generate_series(1, $2::integer)
			RETURNING offer_id
		), closed AS (
			INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate,
				unlock_fee, deposit, status, created_at, started_at, finish_time, closed_at, delete_after, cost)
			SELECT gen_random_uuid(), offer_id, 'user', 'scooter', 'zone-1', 15, 40, 0, 'COMPLETED',
				statement_timestamp() - interval '3 minutes', statement_timestamp() - interval '2 minutes',
                statement_timestamp(), statement_timestamp(), $1, 70 FROM offer
			RETURNING order_id, offer_id, started_at, finish_time
		), completed_tasks AS (
			INSERT INTO tasks (task_id, order_id, task_type, status, parameters, result, created_at, next_run_at, finished_at)
			SELECT gen_random_uuid(), order_id, 'SETTLE', 'COMPLETED',
				jsonb_build_object('operation_id', order_id, 'user_id', 'user', 'total_amount', 70), 'success',
				statement_timestamp(), statement_timestamp(), statement_timestamp() FROM closed WHERE $3::boolean
		)
		SELECT order_id::text, offer_id::text, started_at, finish_time FROM closed`, deleteAfter, count, withCompletedTasks)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTripIDs)
}

func scanTripIDs(row pgx.CollectableRow) (tripIDs, error) {
	var trip tripIDs
	err := row.Scan(&trip.order, &trip.offer, &trip.started, &trip.finished)
	return trip, err
}
