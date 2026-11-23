// Package domain holds Rental's entities, invariants and pricing rules. It has no I/O.
package domain

import "fmt"

// Code is a client-visible error code from the ADR.
type Code string

const (
	CodeInvalidRequest    Code = "INVALID_REQUEST"
	CodeNotFound          Code = "NOT_FOUND"
	CodeParameterConflict Code = "PARAMETER_CONFLICT"
	CodeOfferExpired      Code = "OFFER_EXPIRED"
	CodeOfferChanged      Code = "OFFER_CHANGED"
	CodeScooterBusy       Code = "SCOOTER_BUSY"
	CodeOrderNotActive    Code = "ORDER_NOT_ACTIVE"
	CodeSourceUnavailable Code = "SOURCE_UNAVAILABLE"
	CodeZoneStale         Code = "ZONE_STALE"
)

// Error is a business error that maps to a client response.
type Error struct {
	Code Code
}

func (e *Error) Error() string { return string(e.Code) }

// Is makes errors.Is match any *Error with the same code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

var (
	ErrInvalidRequest    = &Error{Code: CodeInvalidRequest}
	ErrNotFound          = &Error{Code: CodeNotFound}
	ErrParameterConflict = &Error{Code: CodeParameterConflict}
	ErrOfferExpired      = &Error{Code: CodeOfferExpired}
	ErrOfferChanged      = &Error{Code: CodeOfferChanged}
	ErrScooterBusy       = &Error{Code: CodeScooterBusy}
	ErrOrderNotActive    = &Error{Code: CodeOrderNotActive}
	ErrSourceUnavailable = &Error{Code: CodeSourceUnavailable}
	ErrZoneStale         = &Error{Code: CodeZoneStale}
)

// Wrap attaches a detail to a business error while keeping errors.Is/As working.
func Wrap(err *Error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", err, fmt.Sprintf(format, args...))
}
