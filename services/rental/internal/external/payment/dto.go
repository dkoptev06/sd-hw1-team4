package payment

import (
	"math/big"
	"time"
)

type holdRequest struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

type settleRequest struct {
	OperationID string   `json:"operation_id"`
	UserID      string   `json:"user_id"`
	TotalAmount *big.Int `json:"total_amount"`
}

type voidRequest struct {
	OperationID string `json:"operation_id"`
}
