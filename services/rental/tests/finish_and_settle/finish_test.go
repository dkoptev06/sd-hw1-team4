//go:build functional

package finish_and_settle

import (
	"encoding/json"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkFinishedTrips(t *testing.T, rentalBinary, paymentBinary string) {
	for _, test := range []struct {
		name    string
		deposit int64
		expired bool
		short   bool
	}{
		{name: "SETTLE без HOLD"},
		{name: "SETTLE после истечения HOLD", deposit: 250, expired: true},
		{name: "Поездка короче пяти секунд", deposit: 250, short: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var apiEnv []string
			if test.expired {
				apiEnv = []string{"START_TTL=2s", "HOLD_GRACE=1s"}
			}
			s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{}, apiEnv...)
			worker := s.StartWorker(t)
			user, paymentStatusBefore := "user-1", "HELD"
			if test.deposit == 0 {
				user, paymentStatusBefore = "trusted-user", "NOT_FOUND"
			}
			orderID := tests.StartTrip(t, s.Client, s.API, tests.OfferRequest{
				ScooterID: "scooter", UserID: user, IdempotencyID: "finish",
			}, test.deposit)
			tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
			worker.Stop()
			var calls []tests.PaymentCall
			if test.deposit > 0 {
				calls = append(calls, tests.PaymentCall{Method: "hold", OperationID: orderID})
			}
			if test.expired {
				tests.WaitFor(t, "истечения HOLD перед finish", func() (bool, error) {
					status, got, err := s.PaymentStatus(orderID)
					return status == http.StatusOK && got.Status == "HOLD_EXPIRED", err
				})
			} else if test.deposit == 0 {
				s.RequirePayment(t, orderID, http.StatusNotFound, paymentStatusBefore)
			} else {
				s.RequirePayment(t, orderID, http.StatusOK, paymentStatusBefore)
			}

			finished := s.FinishTrip(t, orderID, test.short)
			pending := s.WaitPaymentTask(t, orderID, "SETTLE", "PENDING")
			if pending.Attempts != 0 || pending.Claimed || pending.Result != "" || pending.LastError != "" {
				t.Fatalf("SETTLE до запуска worker: %+v", pending)
			}
			requireFinishReplay(t, s, finished)
			tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), calls...)

			s.StartWorker(t)
			task := s.WaitPaymentTask(t, orderID, "SETTLE", "COMPLETED")
			if task.Result != "success" || task.Attempts != 1 || task.Claimed ||
				task.ID != pending.ID || task.Parameters != pending.Parameters {
				t.Fatalf("неожиданный итог SETTLE: %+v", task)
			}
			s.RequirePayment(t, orderID, http.StatusOK, "PAID")
			requireFinishReplay(t, s, finished)

			calls = append(calls, tests.PaymentCall{Method: "settle", OperationID: orderID})
			attempts := s.Proxy.RecordedAttempts()
			tests.RequirePaymentCommands(t, attempts, calls...)
			requireSettle(t, attempts[len(attempts)-1], orderID, user, *finished.Cost)
		})
	}
}

func requireFinishReplay(t *testing.T, s *tests.PaymentStand, finished tests.FinishResponse) {
	t.Helper()
	readSettle := func() string {
		t.Helper()
		var task string
		err := s.DB.QueryRow(t.Context(), `
			SELECT row_to_json(t)::text FROM tasks t
			WHERE order_id = $1 AND task_type = 'SETTLE'`, finished.OrderID).Scan(&task)
		if err != nil {
			t.Fatal(err)
		}
		return task
	}

	taskBefore := readSettle()
	s.RepeatFinish(t, finished)
	if taskAfter := readSettle(); taskAfter != taskBefore {
		t.Fatalf("повтор finish изменил SETTLE: было %s, стало %s", taskBefore, taskAfter)
	}
	status, order, err := tests.RequestJSON[tests.OrderResponse](s.Client, http.MethodGet,
		s.API+"/orders/"+finished.OrderID, nil)
	if err != nil || status != http.StatusOK || order.OrderID != finished.OrderID || order.Status != finished.Status ||
		order.Cost == nil || *order.Cost != *finished.Cost || order.FinishTime == nil ||
		!order.FinishTime.Equal(*finished.FinishTime) || order.StartedAt == nil || order.FinishTime.Before(*order.StartedAt) {
		t.Fatalf("GET после finish: HTTP %d, %+v, ошибка %v, ожидался %+v", status, order, err, finished)
	}
}

func checkSettleInsertRollback(t *testing.T, rentalBinary, paymentBinary string) {
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{})
	worker := s.StartWorker(t)
	orderID := tests.StartTrip(t, s.Client, s.API, tests.OfferRequest{
		ScooterID: "scooter", UserID: "trusted-user", IdempotencyID: "settle-rollback",
	}, 0)
	tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	worker.Stop()

	var before string
	if err := s.DB.QueryRow(t.Context(),
		`SELECT row_to_json(o)::text FROM orders o WHERE order_id = $1`, orderID,
	).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Вызов nextval остаётся видимым после отката транзакции.
	_, err := s.DB.Exec(t.Context(), `
		CREATE SEQUENCE rejected_settle_insert;
		CREATE FUNCTION reject_settle_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM nextval('rejected_settle_insert');
			RAISE EXCEPTION 'injected SETTLE insert failure';
		END;
		$$;
		CREATE TRIGGER reject_settle_insert BEFORE INSERT ON tasks
			FOR EACH ROW WHEN (NEW.task_type = 'SETTLE')
			EXECUTE FUNCTION reject_settle_insert();
	`)
	if err != nil {
		t.Fatal(err)
	}
	failed := tests.RequirePost[tests.OrderResponse](t, s.Client,
		s.API+"/orders/"+orderID+"/finish", nil, http.StatusInternalServerError)
	if failed.ErrorCode != "INTERNAL_ERROR" {
		t.Fatalf("ответ при ошибке вставки SETTLE: %+v", failed)
	}

	var after string
	var attempted bool
	var settleTasks int
	err = s.DB.QueryRow(t.Context(), `
		SELECT row_to_json(o)::text, (SELECT is_called FROM rejected_settle_insert),
			(SELECT count(*) FROM tasks WHERE order_id = $1 AND task_type = 'SETTLE')
		FROM orders o WHERE o.order_id = $1`, orderID,
	).Scan(&after, &attempted, &settleTasks)
	if err != nil || !attempted || after != before || settleTasks != 0 {
		t.Fatalf("после ошибки SETTLE: вставка дошла до триггера %t, задач %d, заказ до %s, после %s, ошибка %v",
			attempted, settleTasks, before, after, err)
	}
	tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts())

	if _, err := s.DB.Exec(t.Context(), `DROP TRIGGER reject_settle_insert ON tasks`); err != nil {
		t.Fatal(err)
	}
	finished := s.FinishTrip(t, orderID, false)
	s.StartWorker(t)
	s.WaitPaymentTask(t, orderID, "SETTLE", "COMPLETED")
	s.RequirePayment(t, orderID, http.StatusOK, "PAID")
	attempts := s.Proxy.RecordedAttempts()
	tests.RequirePaymentCommands(t, attempts, tests.PaymentCall{Method: "settle", OperationID: orderID})
	requireSettle(t, attempts[0], orderID, "trusted-user", *finished.Cost)
}

func checkLargeTripCost(t *testing.T, rentalBinary, paymentBinary string) {
	const scooter, user = "large-fare", "user-1"
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/configs":
			tests.WriteSourceConfig(w, 4, map[string]int{}, 10)
		case "/tariff-zone/zone-1":
			tests.WriteJSON(w, map[string]any{
				"id": "zone-1", "price_per_minute": int64(1 << 61), "price_unlock": 40, "default_deposit": 250,
			})
		case "/scooter/" + scooter:
			tests.WriteJSON(w, map[string]any{"id": scooter, "zone_id": "zone-1", "charge": 27})
		default:
			return false
		}
		return true
	})
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{}, "SOURCE_URL="+source.URL)
	s.StartWorker(t)
	offer := tests.RequirePost[tests.OfferResponse](t, s.Client, s.API+"/offers", tests.OfferRequest{
		ScooterID: scooter, UserID: user, IdempotencyID: "large-fare",
	}, http.StatusCreated)
	const minuteRate = (1 << 63) * 4 / 5
	if offer.MinuteRate != minuteRate || offer.UnlockFee != 40 || offer.Deposit != 250 {
		t.Fatalf("условия дорогого оффера: %+v", offer)
	}
	start := tests.RequirePost[tests.StartResponse](t, s.Client, s.API+"/orders", tests.StartRequest{OfferID: offer.OfferID}, http.StatusAccepted)
	orderID := start.OrderID
	tests.WaitForOrder(t, s.Client, s.API, orderID, "ACTIVE")
	s.RequirePayment(t, orderID, http.StatusOK, "HELD")
	var startedAt time.Time
	if err := s.DB.QueryRow(t.Context(), `
		UPDATE orders SET started_at = clock_timestamp() - interval '75 seconds'
		WHERE order_id = $1 RETURNING started_at`, orderID).Scan(&startedAt); err != nil {
		t.Fatal(err)
	}

	url := s.API + "/orders/" + orderID
	type largeCostResponse struct {
		OrderID    string     `json:"order_id"`
		Status     string     `json:"status"`
		Cost       *big.Int   `json:"cost"`
		FinishTime *time.Time `json:"finish_time"`
	}
	status, active, err := tests.RequestJSON[largeCostResponse](s.Client, http.MethodGet, url, nil)
	if err != nil || status != http.StatusOK || active.OrderID != orderID || active.Status != "ACTIVE" ||
		active.Cost == nil || active.Cost.IsInt64() {
		t.Fatalf("GET дорогой активной поездки: HTTP %d, %+v, %v", status, active, err)
	}
	finished := tests.RequirePost[largeCostResponse](t, s.Client, url+"/finish", nil, http.StatusOK)
	if finished.FinishTime == nil {
		t.Fatalf("finish без времени завершения: %+v", finished)
	}
	seconds := int64(finished.FinishTime.Sub(startedAt) / time.Second)
	wantCost := new(big.Int).Mul(big.NewInt(seconds), big.NewInt(minuteRate))
	wantCost.Quo(wantCost, big.NewInt(60))
	wantCost.Add(wantCost, big.NewInt(40))
	if finished.OrderID != orderID || finished.Status != "COMPLETED" || finished.Cost == nil ||
		finished.Cost.IsInt64() || finished.Cost.Cmp(wantCost) != 0 {
		t.Fatalf("finish дорогой поездки: %+v, ожидаемая стоимость %s", finished, wantCost)
	}
	task := s.WaitPaymentTask(t, orderID, "SETTLE", "COMPLETED")
	if task.Attempts != 1 || task.Result != "success" {
		t.Fatalf("итог SETTLE: %+v", task)
	}
	s.RequirePayment(t, orderID, http.StatusOK, "PAID")
	attempts := s.Proxy.RecordedAttempts()
	tests.RequirePaymentCommands(t, attempts,
		tests.PaymentCall{Method: "hold", OperationID: orderID},
		tests.PaymentCall{Method: "settle", OperationID: orderID})
	var settle struct {
		UserID      string   `json:"user_id"`
		TotalAmount *big.Int `json:"total_amount"`
	}
	if err := json.Unmarshal([]byte(attempts[1].Body), &settle); err != nil || settle.UserID != user ||
		settle.TotalAmount == nil || settle.TotalAmount.Cmp(wantCost) != 0 {
		t.Fatalf("точная стоимость не дошла до Payment: %s, %v", attempts[1].Body, err)
	}

	newID := tests.StartTrip(t, s.Client, s.API, tests.OfferRequest{
		ScooterID: scooter, UserID: user, IdempotencyID: "after-large-fare",
	}, 250)
	if newID == orderID {
		t.Fatalf("ожидался новый заказ на освободившийся самокат, получен %s", newID)
	}

	s.StartRental(t, "maintenance", "MAINTENANCE_INTERVAL=100ms", "MAINTENANCE_PARTITION_WEEKS_AHEAD=1")
	tests.WaitFor(t, "переноса дорогой поездки в историю", func() (bool, error) {
		var archived bool
		err := s.DB.QueryRow(t.Context(),
			`SELECT EXISTS (SELECT 1 FROM orders_history WHERE order_id = $1)`, orderID).Scan(&archived)
		return archived, err
	})
	status, archived, err := tests.RequestJSON[largeCostResponse](s.Client, http.MethodGet, url, nil)
	if err != nil || status != http.StatusOK || archived.OrderID != orderID || archived.Status != "COMPLETED" ||
		archived.Cost == nil || archived.Cost.Cmp(wantCost) != 0 {
		t.Fatalf("GET дорогой поездки из истории: HTTP %d, %+v, %v", status, archived, err)
	}
	replayed := tests.RequirePost[largeCostResponse](t, s.Client, url+"/finish", nil, http.StatusOK)
	if replayed.OrderID != orderID || replayed.Status != "COMPLETED" || replayed.Cost == nil ||
		replayed.Cost.Cmp(wantCost) != 0 || replayed.FinishTime == nil || !replayed.FinishTime.Equal(*finished.FinishTime) {
		t.Fatalf("повтор finish дорогой поездки из истории: %+v", replayed)
	}
}
