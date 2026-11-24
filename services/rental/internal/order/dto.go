package order

import (
	"math/big"
	"time"
)

type startOrderRequest struct {
	OfferID string `json:"offer_id"`
}

type startOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type orderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	Cost       *big.Int   `json:"cost"`
	StartedAt  *time.Time `json:"started_at"`
	FinishTime *time.Time `json:"finish_time"`
}

type finishOrderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	FinishTime *time.Time `json:"finish_time"`
	Cost       *big.Int   `json:"cost"`
}
