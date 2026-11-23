package offer

import (
	"errors"
	"testing"
	"time"
)

func TestSnapshotConstructorAndStart(t *testing.T) {
	for _, charge := range []int{-1, 101} {
		if _, err := NewScooterSnapshot("scooter", "zone", charge); err == nil {
			t.Fatalf("accepted invalid charge %d", charge)
		}
	}
	snapshot, err := NewScooterSnapshot("scooter", "zone", 28)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewQuote(snapshot, "user", "key", 50, 0, -1, false); err == nil {
		t.Fatal("accepted negative deposit")
	}
	if _, err := NewQuote(snapshot, "user", "", 50, 0, 0, false); err == nil {
		t.Fatal("accepted empty idempotency ID")
	}
	ready, err := NewQuote(snapshot, "user", "key", 50, 0, 0, false)
	if err != nil || ready.deposit != 0 {
		t.Fatalf("quote with zero deposit: %+v, %v", ready, err)
	}

	expires := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	available, err := newStartSnapshot(snapshot, "user", 50, 0, 0, "AVAILABLE", expires)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, zone string
		charge     int
		now        time.Time
		want       error
	}{
		{"same charge category", "zone", 50, expires.Add(-time.Second), nil},
		{"different charge category", "zone", 27, expires.Add(-time.Second), ErrChanged},
		{"different zone", "changed", 28, expires.Add(-time.Second), ErrChanged},
		{"expiration boundary", "zone", 28, expires, ErrExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			current, err := NewScooterSnapshot("scooter", test.zone, test.charge)
			if err != nil {
				t.Fatal(err)
			}
			if got := available.CanStart(test.now, current); !errors.Is(got, test.want) {
				t.Fatalf("CanStart = %v, want %v", got, test.want)
			}
		})
	}
}
