package offer

import "errors"

var (
	ErrInvalidRequest    = errors.New("INVALID_REQUEST")
	ErrNotFound          = errors.New("NOT_FOUND")
	ErrParameterConflict = errors.New("PARAMETER_CONFLICT")
	ErrOfferExpired      = errors.New("OFFER_EXPIRED")
	ErrOfferChanged      = errors.New("OFFER_CHANGED")
	ErrSourceUnavailable = errors.New("SOURCE_UNAVAILABLE")
)
