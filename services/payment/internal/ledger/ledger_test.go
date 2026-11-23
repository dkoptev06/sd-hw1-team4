package ledger

import (
	"errors"
	"testing"
	"time"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newLedger() (*Ledger, *clock) {
	c := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	return New(c.Now), c
}

func must(t *testing.T, got string, err error, want string) {
	t.Helper()
	if err != nil || got != want {
		t.Fatalf("got %q, %v; want %q", got, err, want)
	}
}

func fails(t *testing.T, _ string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
}

func TestHoldThenSettle(t *testing.T) {
	l, c := newLedger()
	expires := c.now.Add(time.Hour)
	r, err := l.Hold("op", "u", 300, expires)
	must(t, r, err, Success)
	r, err = l.Hold("op", "u", 300, expires)
	must(t, r, err, Success) // repeat returns the stored result
	r, err = l.Status("op")
	must(t, r, err, StatusHeld)

	r, err = l.Hold("op", "u", 301, expires)
	fails(t, r, err, ErrParameterConflict)
	r, err = l.Settle("op", "other", 100)
	fails(t, r, err, ErrUserMismatch)

	r, err = l.Settle("op", "u", 100)
	must(t, r, err, Success)
	r, err = l.Settle("op", "u", 100)
	must(t, r, err, Success)
	r, err = l.Settle("op", "u", 90)
	fails(t, r, err, ErrParameterConflict)
	r, err = l.Void("op")
	fails(t, r, err, ErrInvalidOrder)
	r, err = l.Status("op")
	must(t, r, err, StatusPaid)
}

func TestVoidForbidsLateHold(t *testing.T) {
	l, c := newLedger()
	r, err := l.Void("op")
	must(t, r, err, Success)
	r, err = l.Void("op")
	must(t, r, err, Success)
	r, err = l.Hold("op", "u", 300, c.now.Add(time.Hour))
	must(t, r, err, Failed)
	r, err = l.Settle("op", "u", 0)
	fails(t, r, err, ErrInvalidOrder)
	r, err = l.Status("op")
	must(t, r, err, StatusVoided)
}

func TestHoldExpiry(t *testing.T) {
	l, c := newLedger()
	r, err := l.Hold("late", "u", 300, c.now)
	must(t, r, err, Failed)
	r, err = l.Status("late")
	must(t, r, err, StatusHoldFailed)

	r, err = l.Hold("op", "u", 300, c.now.Add(time.Minute))
	must(t, r, err, Success)
	c.now = c.now.Add(time.Minute)
	r, err = l.Status("op")
	must(t, r, err, StatusHoldExpired)
	r, err = l.Settle("op", "u", 50) // settling without a live hold is allowed
	must(t, r, err, Success)
}

func TestValidationAndUnknown(t *testing.T) {
	l, c := newLedger()
	r, err := l.Hold("op", "u", 0, c.now.Add(time.Hour))
	fails(t, r, err, ErrInvalid)
	r, err = l.Settle("op", "", 10)
	fails(t, r, err, ErrInvalid)
	r, err = l.Status("missing")
	fails(t, r, err, ErrNotFound)
	r, err = l.Settle("no-hold", "u", 0)
	must(t, r, err, Success)
}
