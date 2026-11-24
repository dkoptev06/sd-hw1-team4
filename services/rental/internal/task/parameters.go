package task

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

// These records describe the parameters persisted in tasks.parameters.
type holdRecord struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

type settleRecord struct {
	OperationID string   `json:"operation_id"`
	UserID      string   `json:"user_id"`
	TotalAmount *big.Int `json:"total_amount"`
}

type voidRecord struct {
	OperationID string `json:"operation_id"`
}

func decodeParameters(raw json.RawMessage, dst any) error {
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid task parameters: %w", err)
	}
	return nil
}
