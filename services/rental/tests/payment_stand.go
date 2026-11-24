//go:build functional

package tests

import (
	"net/http"
	"testing"
	"time"
)

type PaymentStand struct {
	*Stand
	API, PaymentURL, sourceURL string
	Proxy                      *PaymentProxy
	Client                     *http.Client
}

func NewPaymentStand(t *testing.T, rentalBinary, paymentBinary string, faults PaymentFaults, apiEnv ...string) *PaymentStand {
	t.Helper()
	s := NewStand(t, rentalBinary)
	source := NewSourceStub(t)
	paymentURL := startService(t, paymentBinary, "PAYMENT_HTTP_ADDR", "/healthz", nil).URL
	proxy := NewPaymentProxy(t, paymentURL, faults)
	api := s.StartRental(t, "api", append([]string{"SOURCE_URL=" + source.URL, "PAYMENT_URL=" + proxy.URL}, apiEnv...)...).URL
	return &PaymentStand{
		Stand: s, API: api, PaymentURL: paymentURL, sourceURL: source.URL, Proxy: proxy,
		Client: &http.Client{Timeout: 4 * time.Second},
	}
}

func (s *PaymentStand) StartWorker(t *testing.T, env ...string) *ServiceProcess {
	t.Helper()
	return s.StartRental(t, "worker", append([]string{
		"SOURCE_URL=" + s.sourceURL, "PAYMENT_URL=" + s.Proxy.URL,
		"WORKER_POLL_INTERVAL=20ms", "WORKER_SWEEP_INTERVAL=20ms",
		"WORKER_BACKOFF_BASE=30ms", "WORKER_BACKOFF_MAX=150ms",
	}, env...)...)
}

func (s *PaymentStand) StartOrder(t *testing.T) string {
	t.Helper()
	return StartTrip(t, s.Client, s.API,
		OfferRequest{ScooterID: "scooter", UserID: "user-1", IdempotencyID: "offer"}, 250)
}

type PaymentStatus struct {
	Status    string `json:"status"`
	ErrorCode string `json:"error_code"`
}

func (s *PaymentStand) PaymentStatus(orderID string) (int, PaymentStatus, error) {
	return RequestJSON[PaymentStatus](s.Client, http.MethodGet, s.PaymentURL+"/payment/"+orderID, nil)
}

func (s *PaymentStand) RequirePayment(t *testing.T, orderID string, wantHTTP int, want string) {
	t.Helper()
	status, body, err := s.PaymentStatus(orderID)
	got := body.Status
	if wantHTTP != http.StatusOK {
		got = body.ErrorCode
	}
	if err != nil || status != wantHTTP || got != want {
		t.Fatalf("GET /payment/%s: HTTP %d, %+v, %v. Ожидалось HTTP %d %s", orderID, status, body, err, wantHTTP, want)
	}
	t.Logf("Payment %s вернул HTTP %d, %s", orderID, status, got)
}

func WaitForStaleStart(t *testing.T, client *http.Client, workerURL string) {
	t.Helper()
	WaitForMetric(t, client, workerURL, `rental_task_stale_claims_total{kind="START"} 1`)
}

func (s *PaymentStand) RequireFailedStart(t *testing.T, orderID, reason string, attempts int) {
	t.Helper()
	order := WaitForOrder(t, s.Client, s.API, orderID, "START_FAILED")
	if order.Cost == nil || *order.Cost != 0 {
		t.Fatalf("стоимость неудачного старта: %+v", order)
	}
	s.WaitForVoid(t, orderID)
	start := s.ReadStart(t, orderID)
	if start.Status != "COMPLETED" || start.Reason != reason || start.Attempts != attempts || start.ClaimID != "" {
		t.Fatalf("итог START: %+v, ожидались %s и %d попыток", start, reason, attempts)
	}
	s.RequirePayment(t, orderID, http.StatusOK, "VOIDED")
}

func (s *PaymentStand) FinishTrip(t *testing.T, orderID string, short bool) FinishResponse {
	t.Helper()
	age := 61.0
	if short {
		age = 0
	}
	// Сдвигаем время старта в отдельной тестовой схеме, без долгого ожидания поездки.
	var startedAt time.Time
	err := s.DB.QueryRow(t.Context(), `
		UPDATE orders SET started_at = clock_timestamp() - make_interval(secs => $2)
		WHERE order_id = $1 AND status = 'ACTIVE' RETURNING started_at`, orderID, age).Scan(&startedAt)
	if err != nil {
		t.Fatal(err)
	}
	finished := RequirePost[FinishResponse](t, s.Client, s.API+"/orders/"+orderID+"/finish", nil, http.StatusOK)
	if finished.OrderID != orderID || finished.Cost == nil || finished.FinishTime == nil {
		t.Fatalf("неполный ответ finish: %+v", finished)
	}
	elapsed := finished.FinishTime.Sub(startedAt)
	wantStatus, wantCost := "COMPLETED", int64(40)+int64(elapsed/time.Second)*15/60
	if short {
		wantStatus, wantCost = "CANCELLED", 0
	}
	if finished.Status != wantStatus || *finished.Cost != wantCost {
		t.Fatalf("после %v ожидались %s и стоимость %d, получен %+v", elapsed, wantStatus, wantCost, finished)
	}
	t.Logf("Завершение заказа %s вернуло %s, стоимость %d", orderID, finished.Status, *finished.Cost)
	return finished
}

func (s *PaymentStand) RepeatFinish(t *testing.T, want FinishResponse) {
	t.Helper()
	got := RequirePost[FinishResponse](t, s.Client, s.API+"/orders/"+want.OrderID+"/finish", nil, http.StatusOK)
	if got.OrderID != want.OrderID || got.Status != want.Status ||
		got.Cost == nil || *got.Cost != *want.Cost || got.FinishTime == nil || !got.FinishTime.Equal(*want.FinishTime) {
		t.Fatalf("повтор finish изменил результат: было %+v, стало %+v", want, got)
	}
}
