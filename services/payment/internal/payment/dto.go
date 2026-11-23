package payment

import "time"

type HoldRequest struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

type SettleRequest struct {
	OperationID string `json:"operation_id"`
	UserID      string `json:"user_id"`
	TotalAmount int64  `json:"total_amount"`
}

type VoidRequest struct {
	OperationID string `json:"operation_id"`
}

type CommandResponse struct {
	Status string `json:"status"`
}

type PaymentResponse struct {
	Status string `json:"status"`
}

type ErrorResponse struct {
	ErrorCode string `json:"error_code"`
}
