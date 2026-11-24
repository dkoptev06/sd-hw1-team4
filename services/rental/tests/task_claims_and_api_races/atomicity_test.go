//go:build functional

package task_claims_and_api_races

import (
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkStartCompletionRollback(t *testing.T, rentalBinary, paymentBinary string) {
	s := tests.NewPaymentStand(t, rentalBinary, paymentBinary, tests.PaymentFaults{})
	orderID := s.StartOrder(t)
	s.ExpireStart(t, orderID)
	// Последовательность показывает, что запись была попытана, даже если транзакция откатилась.
	_, err := s.DB.Exec(t.Context(), `
		CREATE SEQUENCE rejected_start_completion;
		CREATE FUNCTION reject_start_completion() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM nextval('rejected_start_completion');
			RAISE EXCEPTION 'injected START completion failure';
		END;
		$$;
		CREATE TRIGGER reject_start_completion BEFORE UPDATE ON tasks
			FOR EACH ROW WHEN (NEW.task_type = 'START' AND NEW.status = 'COMPLETED')
			EXECUTE FUNCTION reject_start_completion();
	`)
	if err != nil {
		t.Fatal(err)
	}
	s.StartWorker(t)
	tests.WaitFor(t, "попытки записи COMPLETED", func() (bool, error) {
		var attempted bool
		err := s.DB.QueryRow(t.Context(), `SELECT is_called FROM rejected_start_completion`).Scan(&attempted)
		return attempted, err
	})
	tests.WaitForOrder(t, s.Client, s.API, orderID, "STARTING")
	s.RequirePendingStart(t, orderID)
	s.RequireNoVoid(t, orderID)
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts())

	if _, err := s.DB.Exec(t.Context(), `DROP TRIGGER reject_start_completion ON tasks`); err != nil {
		t.Fatal(err)
	}
	s.RequireFailedStart(t, orderID, "DEADLINE_EXCEEDED", 0)
	tests.RequirePaymentCommands(t, s.Proxy.RecordedAttempts(), tests.PaymentCall{Method: "void", OperationID: orderID})
}
