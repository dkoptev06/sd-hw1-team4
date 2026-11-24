//go:build functional

package zero_deposit_and_expired_start

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func startWorker(t *testing.T, s *tests.Stand, sourceURL, paymentURL string) {
	t.Helper()
	s.StartRental(t, "worker", "SOURCE_URL="+sourceURL, "PAYMENT_URL="+paymentURL,
		"WORKER_POLL_INTERVAL=20ms", "WORKER_SWEEP_INTERVAL=20ms")
}
