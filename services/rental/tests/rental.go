//go:build functional

package tests

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

type OfferRequest struct {
	ScooterID     string `json:"scooter_id"`
	UserID        string `json:"user_id"`
	IdempotencyID string `json:"idempotency_id"`
}

type OfferResponse struct {
	OfferID    string    `json:"offer_id"`
	MinuteRate int64     `json:"minute_rate"`
	UnlockFee  int64     `json:"unlock_fee"`
	Deposit    int64     `json:"deposit"`
	ExpiresAt  time.Time `json:"expires_at"`
	ErrorCode  string    `json:"error_code"`
}

type StartRequest struct {
	OfferID string `json:"offer_id"`
}

type StartResponse struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code"`
}

type OrderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	Cost       *int64     `json:"cost"`
	StartedAt  *time.Time `json:"started_at"`
	FinishTime *time.Time `json:"finish_time"`
	ErrorCode  string     `json:"error_code"`
}

func StartTrip(t *testing.T, client *http.Client, api string, request OfferRequest, deposit int64) string {
	t.Helper()
	offer := RequirePost[OfferResponse](t, client, api+"/offers", request, http.StatusCreated)
	if offer.OfferID == "" || offer.Deposit != deposit {
		t.Fatalf("неожиданные условия оффера: %+v", offer)
	}
	started := RequirePost[StartResponse](t, client, api+"/orders", StartRequest{offer.OfferID}, http.StatusAccepted)
	if started.OrderID == "" || started.Status != "STARTING" {
		t.Fatalf("неожиданный ответ старта: %+v", started)
	}
	t.Logf("Заказ %s создан в %s, депозит %d", started.OrderID, started.Status, offer.Deposit)
	return started.OrderID
}

func WaitForOrder(t *testing.T, client *http.Client, api, orderID, wantStatus string) OrderResponse {
	t.Helper()
	var order OrderResponse
	WaitFor(t, "статуса "+wantStatus+" у заказа "+orderID, func() (bool, error) {
		status, got, err := RequestJSON[OrderResponse](client, http.MethodGet, api+"/orders/"+orderID, nil)
		if err != nil {
			return false, err
		}
		if status != http.StatusOK || got.OrderID != orderID {
			return false, fmt.Errorf("GET returned HTTP %d for order %q, want %q", status, got.OrderID, orderID)
		}
		order = got
		return order.Status == wantStatus, nil
	})
	t.Logf("GET заказа %s вернул %s", order.OrderID, order.Status)
	return order
}

type FinishResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	FinishTime *time.Time `json:"finish_time"`
	Cost       *int64     `json:"cost"`
}
