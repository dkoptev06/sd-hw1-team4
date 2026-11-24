package offer

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

func TestOfferCheckStart(t *testing.T) {
	expires := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	request, err := newQuoteRequest("s", "user", "key")
	if err != nil {
		t.Fatal(err)
	}
	q, err := newQuote(uuid.New(), request, source.Scooter{ID: "s", ZoneID: "zone", Charge: 28}, Terms{}, false)
	if err != nil {
		t.Fatal(err)
	}
	before := expires.Add(-time.Second)
	tests := []struct {
		name    string
		status  status
		now     time.Time
		current source.Scooter
		want    error
	}{
		{"same category", available, before, source.Scooter{ID: "s", ZoneID: "zone", Charge: 90}, nil},
		{"crossed threshold", available, before, source.Scooter{ID: "s", ZoneID: "zone", Charge: 27}, ErrOfferChanged},
		{"other zone", available, before, source.Scooter{ID: "s", ZoneID: "other", Charge: 28}, ErrOfferChanged},
		{"expiry boundary", available, expires, source.Scooter{ID: "s", ZoneID: "zone", Charge: 28}, ErrOfferExpired},
		{"consumed", consumed, before, source.Scooter{ID: "s", ZoneID: "zone", Charge: 28}, ErrOfferExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := restoreOffer(q, tt.status, expires.Add(-time.Minute), expires)
			if err != nil {
				t.Fatal(err)
			}
			if err := o.checkStart(tt.now, tt.current); !errors.Is(err, tt.want) {
				t.Fatalf("CheckStart = %v, want %v", err, tt.want)
			}
		})
	}
}
