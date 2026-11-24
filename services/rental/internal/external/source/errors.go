package source

import "errors"

var (
	ErrZoneStale         = errors.New("ZONE_STALE")
	ErrTariffUnavailable = errors.New("SOURCE_UNAVAILABLE")
)
