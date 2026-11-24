//go:build functional

package offers_and_concurrent_start

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

type startAttempt struct {
	offerID string
	status  int
	start   tests.StartResponse
	err     error
}

func checkCompetingOffers(t *testing.T, s *tests.Stand, client *http.Client, api, sourceURL string, results [2]startAttempt) {
	t.Helper()
	var winner, busy int
	var winnerID, loserID, orderID string
	for _, result := range results {
		switch {
		case result.err != nil:
			t.Fatal(result.err)
		case result.status == http.StatusAccepted && result.start.Status == "STARTING" && result.start.OrderID != "":
			winner++
			winnerID, orderID = result.offerID, result.start.OrderID
		case result.status == http.StatusConflict && result.start.ErrorCode == "SCOOTER_BUSY":
			busy++
			loserID = result.offerID
		default:
			t.Fatalf("неожиданный результат конкурентного старта: %+v", result)
		}
	}
	if winner != 1 || busy != 1 {
		t.Fatalf("ожидались один старт и один конфликт, получено %+v", results)
	}
	var consumed, available string
	err := s.DB.QueryRow(t.Context(), `SELECT
		(SELECT status FROM offers WHERE offer_id = $1),
		(SELECT status FROM offers WHERE offer_id = $2)`, winnerID, loserID).Scan(&consumed, &available)
	if err != nil || consumed != "CONSUMED" || available != "AVAILABLE" {
		t.Fatalf("после гонки: победивший оффер %q, отклонённый %q, ошибка %v", consumed, available, err)
	}
	t.Logf("Две реплики: 202 для %s (%s), 409 SCOOTER_BUSY для %s (%s)", winnerID, consumed, loserID, available)
	replay := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: winnerID}, http.StatusOK)
	if replay.OrderID != orderID || replay.Status != "STARTING" {
		t.Fatalf("повтор вернул %+v, ожидался заказ %s", replay, orderID)
	}
	payment := tests.NewPaymentStub(t)
	s.StartRental(t, "worker", "SOURCE_URL="+sourceURL, "PAYMENT_URL="+payment.URL, "WORKER_POLL_INTERVAL=20ms")
	tests.WaitForOrder(t, client, api, orderID, "ACTIVE")
	replay = tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: winnerID}, http.StatusOK)
	if replay.OrderID != orderID || replay.Status != "ACTIVE" {
		t.Fatalf("повтор активного старта вернул %+v, ожидался ACTIVE для %s", replay, orderID)
	}
	loser := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", tests.StartRequest{OfferID: loserID}, http.StatusConflict)
	if loser.ErrorCode != "SCOOTER_BUSY" {
		t.Fatalf("ответ для занятого самоката после активации: %+v", loser)
	}
	var orders, occupied, startTasks int
	err = s.DB.QueryRow(t.Context(), `
		SELECT count(*), count(*) FILTER (WHERE o.status IN ('STARTING', 'ACTIVE')), count(t.task_id)
		FROM orders o LEFT JOIN tasks t ON t.order_id = o.order_id AND t.task_type = 'START'
		WHERE o.scooter_id = $1`, "shared-scooter").Scan(&orders, &occupied, &startTasks)
	if err != nil || orders != 1 || occupied != 1 || startTasks != 1 {
		t.Fatalf("по самокату найдено %d заказов, занятых %d, задач START %d: %v", orders, occupied, startTasks, err)
	}
}

func checkConcurrentStarts(t *testing.T, binary string, client *http.Client, sameOffer bool) {
	s := tests.NewStand(t, binary)
	var blockStarts atomic.Bool
	arrived := make(chan struct{}, 2)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()

	const scooter = "shared-scooter"
	source := tests.NewSourceStubWithOverride(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !blockStarts.Load() || r.URL.Path != "/scooter/"+scooter {
			return false
		}
		select {
		case arrived <- struct{}{}:
		case <-r.Context().Done():
			return true
		}
		select {
		case <-gate:
			return false
		case <-r.Context().Done():
			return true
		}
	})
	apiEnv := []string{"SOURCE_URL=" + source.URL, "SOURCE_TIMEOUT=4s"}
	first := s.StartRental(t, "api", apiEnv...).URL
	second := s.StartRental(t, "api", apiEnv...).URL
	createOffer := func(user string) string {
		t.Helper()
		offer := tests.RequirePost[tests.OfferResponse](t, client, first+"/offers", tests.OfferRequest{
			ScooterID: scooter, UserID: user, IdempotencyID: user,
		}, http.StatusCreated)
		return offer.OfferID
	}
	firstID := createOffer("user-1")
	ids := [2]string{firstID, firstID}
	if !sameOffer {
		ids[1] = createOffer("user-2")
	}

	blockStarts.Store(true)
	results := make(chan startAttempt, 2)
	for i, api := range []string{first, second} {
		go func() {
			status, body, err := tests.RequestJSON[tests.StartResponse](client, http.MethodPost, api+"/orders",
				tests.StartRequest{OfferID: ids[i]})
			results <- startAttempt{offerID: ids[i], status: status, start: body, err: err}
		}()
	}

	// Обе реплики должны пройти первый поиск заказа до того, как любая создаст его.
	wait, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for range 2 {
		select {
		case <-arrived:
		case result := <-results:
			t.Fatalf("запрос завершился до освобождения барьера Source: %+v", result)
		case <-wait.Done():
			t.Fatalf("оба старта должны дойти до Source: %v", wait.Err())
		}
	}
	blockStarts.Store(false)
	release()

	a, b := <-results, <-results
	if !sameOffer {
		checkCompetingOffers(t, s, client, first, source.URL, [2]startAttempt{a, b})
		return
	}
	if b.status == http.StatusAccepted {
		a, b = b, a
	}
	if a.err != nil || b.err != nil || a.status != http.StatusAccepted || b.status != http.StatusOK ||
		a.start.Status != "STARTING" || b.start.Status != "STARTING" ||
		a.start.OrderID == "" || a.start.OrderID != b.start.OrderID {
		t.Fatalf("ожидались 202 и 200 с одним заказом STARTING: %+v и %+v", a, b)
	}

	var orders, startTasks int
	err := s.DB.QueryRow(t.Context(), `
		SELECT count(DISTINCT o.order_id), count(t.task_id)
		FROM orders o LEFT JOIN tasks t ON t.order_id = o.order_id AND t.task_type = 'START'
		WHERE o.offer_id = $1`, firstID).Scan(&orders, &startTasks)
	if err != nil || orders != 1 || startTasks != 1 {
		t.Fatalf("по офферу %s ожидались один заказ и одна задача START, найдено %d и %d, ошибка %v",
			firstID, orders, startTasks, err)
	}
	s.RequirePendingStart(t, a.start.OrderID)
}
