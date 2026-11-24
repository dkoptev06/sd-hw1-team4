//go:build functional

package zero_deposit_and_expired_start

import (
	"net/http"
	"slices"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkExpiredStartReplacement(t *testing.T, s *tests.Stand, client *http.Client, api string, source *tests.SourceStub, payment *tests.PaymentStub) {
	oldID := tests.StartTrip(t, client, api, tests.OfferRequest{
		ScooterID: "replacement", UserID: "user-1", IdempotencyID: "previous",
	}, 250)
	s.ExpireStart(t, oldID)
	before := s.RequirePendingStart(t, oldID)

	nextOffer := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", tests.OfferRequest{
		ScooterID: "replacement", UserID: "user-2", IdempotencyID: "next",
	}, http.StatusCreated)
	startRequest := tests.StartRequest{OfferID: nextOffer.OfferID}
	checkReplacementRollback(t, s, client, api, oldID, startRequest, before)

	newOrder := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", startRequest, http.StatusAccepted)
	newID := newOrder.OrderID
	if newID == "" || newID == oldID || newOrder.Status != "STARTING" {
		t.Fatalf("неожиданный ответ замены заказа %s: %+v", oldID, newOrder)
	}
	oldOrder := tests.WaitForOrder(t, client, api, oldID, "START_FAILED")
	if oldOrder.Cost == nil || *oldOrder.Cost != 0 {
		t.Fatalf("стоимость замещённого заказа: %+v", oldOrder)
	}
	if after := s.ReadStart(t, oldID); after != before {
		t.Fatalf("API изменил старую задачу START: было %+v, стало %+v", before, after)
	}
	s.RequirePendingStart(t, newID)
	s.RequireNoVoid(t, oldID)
	if calls := payment.RecordedCalls(); len(calls) != 0 {
		t.Fatalf("API вызвал Payment до запуска worker: %+v", calls)
	}

	startWorker(t, s, source.URL, payment.URL)
	tests.WaitForOrder(t, client, api, newID, "ACTIVE")
	s.WaitForVoid(t, oldID)
	start := s.ReadStart(t, oldID)
	if start.ID != before.ID || start.Status != "COMPLETED" || start.Reason != "DEADLINE_EXCEEDED" ||
		start.Attempts != before.Attempts || start.ClaimID != "" {
		t.Fatalf("неожиданный итог старого START: %+v", start)
	}
	calls := payment.RecordedCalls()
	if len(calls) != 2 || !slices.Contains(calls, tests.PaymentCall{Method: "void", OperationID: oldID}) ||
		!slices.Contains(calls, tests.PaymentCall{Method: "hold", OperationID: newID}) {
		t.Fatalf("ожидались один VOID для старого заказа и один HOLD для нового, получены %+v", calls)
	}
}

func checkReplacementRollback(t *testing.T, s *tests.Stand, client *http.Client, api, oldID string,
	request tests.StartRequest, before tests.StartTaskState,
) {
	t.Helper()
	_, err := s.DB.Exec(t.Context(), `
		CREATE SEQUENCE rejected_replacement_start;
		CREATE FUNCTION reject_replacement_start() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM nextval('rejected_replacement_start');
			RAISE EXCEPTION 'injected replacement START insert failure';
		END;
		$$;
		CREATE TRIGGER reject_replacement_start BEFORE INSERT ON tasks
			FOR EACH ROW WHEN (NEW.task_type = 'START') EXECUTE FUNCTION reject_replacement_start();
	`)
	if err != nil {
		t.Fatal(err)
	}
	failed := tests.RequirePost[tests.StartResponse](t, client, api+"/orders", request, http.StatusInternalServerError)
	if failed.ErrorCode != "INTERNAL_ERROR" {
		t.Fatalf("код ошибки при откате замены: %+v", failed)
	}
	var attempted, oldUnchanged, offerAvailable, noReplacement bool
	err = s.DB.QueryRow(t.Context(), `
		SELECT (SELECT is_called FROM rejected_replacement_start),
			(SELECT status = 'STARTING' AND closed_at IS NULL AND delete_after IS NULL AND cost IS NULL
			 FROM orders WHERE order_id = $1),
			(SELECT status = 'AVAILABLE' FROM offers WHERE offer_id = $2),
			NOT EXISTS (SELECT 1 FROM orders WHERE offer_id = $2)
	`, oldID, request.OfferID).Scan(&attempted, &oldUnchanged, &offerAvailable, &noReplacement)
	if err != nil || !attempted || !oldUnchanged || !offerAvailable || !noReplacement {
		t.Fatalf("откат замены: вставка была %t, старый заказ без изменений %t, оффер доступен %t, заказ отсутствует %t, %v",
			attempted, oldUnchanged, offerAvailable, noReplacement, err)
	}
	if after := s.ReadStart(t, oldID); after != before {
		t.Fatalf("откат замены изменил старую задачу START: было %+v, стало %+v", before, after)
	}
	s.RequireNoVoid(t, oldID)
	if _, err := s.DB.Exec(t.Context(), `DROP TRIGGER reject_replacement_start ON tasks`); err != nil {
		t.Fatal(err)
	}
}
