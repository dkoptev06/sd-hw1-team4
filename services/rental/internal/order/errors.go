package order

import "errors"

var (
	ErrInvalidRequest    = errors.New("INVALID_REQUEST")
	ErrOverloaded        = errors.New("OVERLOADED")
	ErrNotFound          = errors.New("NOT_FOUND")
	ErrScooterBusy       = errors.New("SCOOTER_BUSY")
	ErrOrderNotActive    = errors.New("ORDER_NOT_ACTIVE")
	ErrSourceUnavailable = errors.New("SOURCE_UNAVAILABLE")
)
