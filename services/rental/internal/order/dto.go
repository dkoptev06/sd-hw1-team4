package order

import "time"

type StartOrderRequest struct {
	OfferID string `json:"offer_id"`
}

type StartOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type OrderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	Cost       *int64     `json:"cost"`
	StartedAt  *time.Time `json:"started_at"`
	FinishTime *time.Time `json:"finish_time"`
}

type FinishOrderResponse struct {
	OrderID    string    `json:"order_id"`
	Status     string    `json:"status"`
	FinishTime time.Time `json:"finish_time"`
	Cost       int64     `json:"cost"`
}
