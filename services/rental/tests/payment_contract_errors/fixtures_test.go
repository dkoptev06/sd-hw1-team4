//go:build functional

package payment_contract_errors

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func standWithContractError(t *testing.T, rentalBinary, paymentBinary, method string) (*tests.PaymentStand, *atomic.Bool) {
	t.Helper()
	reject := new(atomic.Bool)
	reject.Store(true)
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{
		ResponseStatus: func(path string) int {
			if reject.Load() && path == "/payments/"+method {
				return http.StatusConflict
			}
			return 0
		},
	})
	return s, reject
}

type contractResponse struct {
	suffix      string
	modify      func(*http.Response)
	errorPrefix string
}

func incompleteConflict(response *http.Response) {
	response.ContentLength++
	response.Header.Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
}

func localizedConflict(response *http.Response) {
	// На границе 200 байт оказывается середина кириллической буквы.
	body := "заказ\x00" + strings.Repeat("я", 150)
	response.Body.Close()
	response.Body = io.NopCloser(strings.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
}

func startContractErrorWorker(t *testing.T, stand *tests.PaymentStand, response contractResponse) *tests.ServiceProcess {
	t.Helper()
	// Один слот гарантирует, что контрольный START выполнится после более ранних задач.
	env := []string{"WORKER_CONCURRENCY=1", "START_MAX_ATTEMPTS=3", "SETTLE_MAX_ATTEMPTS=3", "VOID_MAX_ATTEMPTS=3"}
	if response.modify == nil {
		return stand.StartWorker(t, env...)
	}

	target, err := url.Parse(stand.Proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(reply *http.Response) error {
		if reply.StatusCode == http.StatusConflict {
			response.modify(reply)
		}
		return nil
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	return stand.StartWorker(t, append(env, "PAYMENT_URL="+server.URL)...)
}

func waitForNextWorkerPoll(t *testing.T, stand *tests.PaymentStand, orderID, kind string) {
	t.Helper()
	// Дожидаемся, чтобы возможный следующий запуск исходной задачи уже был доступен по времени.
	tests.WaitFor(t, "наступления next_run_at для "+kind, func() (bool, error) {
		var due bool
		err := stand.DB.QueryRow(t.Context(), `
			SELECT next_run_at <= clock_timestamp()
			FROM tasks WHERE order_id = $1 AND task_type = $2`, orderID, kind,
		).Scan(&due)
		return due, err
	})
	controlID := tests.StartTrip(t, stand.Client, stand.API, tests.OfferRequest{
		ScooterID: "poll-control", UserID: "trusted-user", IdempotencyID: "poll-control",
	}, 0)
	tests.WaitForOrder(t, stand.Client, stand.API, controlID, "ACTIVE")
}

func requireContractError(t *testing.T, got string, response contractResponse) {
	t.Helper()
	if !strings.HasPrefix(got, response.errorPrefix) {
		t.Fatalf("ожидалась ошибка %q, получено %q", response.errorPrefix, got)
	}
}
