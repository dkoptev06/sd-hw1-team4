package ledger

import (
	"errors"
	"math/big"
	"testing"
	"time"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newLedger() (*Service, *clock) {
	c := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	return NewService(c.Now, false), c
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
	must(t, r, err, statusHeld)

	r, err = l.Hold("op", "u", 301, expires)
	fails(t, r, err, errParameterConflict)
	r, err = l.Settle("op", "other", big.NewInt(100))
	fails(t, r, err, errUserMismatch)

	r, err = l.Settle("op", "u", big.NewInt(100))
	must(t, r, err, Success)
	r, err = l.Settle("op", "u", big.NewInt(100))
	must(t, r, err, Success)
	r, err = l.Settle("op", "u", big.NewInt(90))
	fails(t, r, err, errParameterConflict)
	r, err = l.Void("op")
	fails(t, r, err, errInvalidOrder)
	r, err = l.Status("op")
	must(t, r, err, statusPaid)
}

func TestVoidForbidsLateHold(t *testing.T) {
	l, c := newLedger()
	r, err := l.Void("op")
	must(t, r, err, Success)
	r, err = l.Void("op")
	must(t, r, err, Success)
	r, err = l.Hold("op", "u", 300, c.now.Add(time.Hour))
	must(t, r, err, Failed)
	r, err = l.Settle("op", "u", big.NewInt(0))
	fails(t, r, err, errInvalidOrder)
	r, err = l.Status("op")
	must(t, r, err, statusVoided)
}

func TestHoldExpiry(t *testing.T) {
	l, c := newLedger()
	r, err := l.Hold("late", "u", 300, c.now)
	must(t, r, err, Failed)
	r, err = l.Status("late")
	must(t, r, err, statusHoldFailed)

	r, err = l.Hold("op", "u", 300, c.now.Add(time.Minute))
	must(t, r, err, Success)
	c.now = c.now.Add(time.Minute)
	r, err = l.Status("op")
	must(t, r, err, statusHoldExpired)
	r, err = l.Settle("op", "u", big.NewInt(50)) // settling without a live hold is allowed
	must(t, r, err, Success)
}

func TestValidationAndUnknown(t *testing.T) {
	l, c := newLedger()
	r, err := l.Hold("op", "u", 0, c.now.Add(time.Hour))
	fails(t, r, err, errInvalid)
	r, err = l.Settle("op", "", big.NewInt(10))
	fails(t, r, err, errInvalid)
	r, err = l.Status("missing")
	fails(t, r, err, errNotFound)
	r, err = l.Settle("no-hold", "u", big.NewInt(0))
	must(t, r, err, Success)
}

func TestSettleStoresCopyOfAmount(t *testing.T) {
	l, _ := newLedger()
	original := new(big.Int).Lsh(big.NewInt(1), 80)
	request := new(big.Int).Set(original)
	result, err := l.Settle("op", "u", request)
	must(t, result, err, Success)

	request.Add(request, big.NewInt(1))
	result, err = l.Settle("op", "u", original)
	must(t, result, err, Success)
	result, err = l.Settle("op", "u", request)
	fails(t, result, err, errParameterConflict)
}
