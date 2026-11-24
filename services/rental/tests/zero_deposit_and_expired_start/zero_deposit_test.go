//go:build functional

package zero_deposit_and_expired_start

import (
	"net/http"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkZeroDeposit(t *testing.T, s *tests.Stand, client *http.Client, api string, source *tests.SourceStub, payment *tests.PaymentStub) {
	for _, test := range []struct {
		name      string
		key       string
		expired   bool
		wantOrder string
		reason    string
		result    string
		attempts  int
	}{
		{name: "Нулевой депозит до дедлайна", key: "zero-live", wantOrder: "ACTIVE", result: "success", attempts: 1},
		{name: "Нулевой депозит после дедлайна", key: "zero-expired", expired: true, wantOrder: "START_FAILED", reason: "DEADLINE_EXCEEDED", result: "failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			orderID := tests.StartTrip(t, client, api, tests.OfferRequest{
				ScooterID: test.key, UserID: "trusted-user", IdempotencyID: test.key,
			}, 0)
			pending := s.RequirePendingStart(t, orderID)
			starting := tests.WaitForOrder(t, client, api, orderID, "STARTING")
			if starting.Cost != nil || starting.StartedAt != nil || starting.FinishTime != nil {
				t.Fatalf("до активации стоимость и время должны быть null: %+v", starting)
			}
			if test.expired {
				s.ExpireStart(t, orderID)
			}
			startWorker(t, s, source.URL, payment.URL)
			view := tests.WaitForOrder(t, client, api, orderID, test.wantOrder)
			start := s.ReadStart(t, orderID)
			if start.ID != pending.ID || start.Status != "COMPLETED" || start.Reason != test.reason ||
				start.Attempts != test.attempts || !start.Result.Valid || start.Result.String != test.result ||
				start.ClaimID != "" || start.LeaseUntil.Valid || !start.FinishedAt.Valid {
				t.Fatalf("неожиданный итог START: %+v", start)
			}
			if view.Cost == nil || view.FinishTime != nil {
				t.Fatalf("неожиданные поля стоимости или времени: %+v", view)
			}
			if test.expired {
				if *view.Cost != 0 || view.StartedAt != nil || start.FinishedAt.Time.Before(start.Deadline) {
					t.Fatalf("просроченный заказ: %+v, задача %+v", view, start)
				}
			} else if view.StartedAt == nil || !view.StartedAt.Equal(start.FinishedAt.Time) || !view.StartedAt.Before(start.Deadline) {
				t.Fatalf("активация до дедлайна: %+v, задача %+v", view, start)
			}
			s.RequireNoVoid(t, orderID)
			if calls := payment.RecordedCalls(); len(calls) != 0 {
				t.Fatalf("при нулевом депозите были вызовы Payment: %+v", calls)
			}
		})
	}
}
