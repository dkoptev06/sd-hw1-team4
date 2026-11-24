//go:build functional

package offers_and_concurrent_start

import (
	"net/http"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func checkOfferReplay(t *testing.T, s *tests.Stand, client *http.Client, api string, source *tests.SourceStub) {
	request := tests.OfferRequest{ScooterID: "priced", UserID: "user-1", IdempotencyID: "original"}
	original := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusCreated)
	if original.OfferID == "" || original.ExpiresAt.IsZero() || original.MinuteRate != 15 ||
		original.UnlockFee != 40 || original.Deposit != 250 {
		t.Fatalf("неожиданные условия оффера: %+v", original)
	}
	replay := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", request, http.StatusOK)
	if replay != original {
		t.Fatalf("повтор вернул %+v, исходный оффер %+v", replay, original)
	}
	changedUser, changedScooter := request, request
	changedUser.UserID = "user-2"
	changedScooter.ScooterID = "other"
	for _, changed := range []tests.OfferRequest{changedUser, changedScooter} {
		conflict := tests.RequirePost[tests.OfferResponse](t, client, api+"/offers", changed, http.StatusConflict)
		if conflict.ErrorCode != "PARAMETER_CONFLICT" {
			t.Fatalf("неожиданный код конфликта для %+v: %q", changed, conflict.ErrorCode)
		}
	}

	source.SetUpdated(true)
	defer source.SetUpdated(false)
	// Вторая реплика читает новый тариф с холодным кэшем.
	replica := s.StartRental(t, "api", "SOURCE_URL="+source.URL).URL
	newRequest := request
	newRequest.IdempotencyID = "after-update"
	newQuote := tests.RequirePost[tests.OfferResponse](t, client, replica+"/offers", newRequest, http.StatusCreated)
	if newQuote.MinuteRate != 35 || newQuote.UnlockFee != 0 || newQuote.Deposit != 0 {
		t.Fatalf("новый оффер не учёл изменённые тариф и профиль: %+v", newQuote)
	}
	replay = tests.RequirePost[tests.OfferResponse](t, client, replica+"/offers", request, http.StatusOK)
	if replay != original {
		t.Fatalf("после обновления Source повтор вернул %+v вместо %+v", replay, original)
	}

	started := tests.RequirePost[tests.StartResponse](t, client, replica+"/orders", tests.StartRequest{OfferID: original.OfferID}, http.StatusAccepted)
	if started.OrderID == "" || started.Status != "STARTING" {
		t.Fatalf("неожиданный ответ старта: %+v", started)
	}
	var rate, unlock, deposit int64
	err := s.DB.QueryRow(t.Context(), `
		SELECT minute_rate, unlock_fee, deposit FROM orders WHERE order_id = $1`, started.OrderID).
		Scan(&rate, &unlock, &deposit)
	if err != nil || rate != original.MinuteRate || unlock != original.UnlockFee || deposit != original.Deposit {
		t.Fatalf("в заказе условия %d/%d/%d вместо условий оффера %+v: %v", rate, unlock, deposit, original, err)
	}
}
