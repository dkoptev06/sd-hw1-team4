package offer

import "time"

type createOfferRequest struct {
	ScooterID     string `json:"scooter_id"`
	UserID        string `json:"user_id"`
	IdempotencyID string `json:"idempotency_id"`
}

type offerResponse struct {
	OfferID    string    `json:"offer_id"`
	MinuteRate int64     `json:"minute_rate"`
	UnlockFee  int64     `json:"unlock_fee"`
	Deposit    int64     `json:"deposit"`
	ExpiresAt  time.Time `json:"expires_at"`
}
