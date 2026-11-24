package order

import (
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func TestTripCost(t *testing.T) {
	terms := offer.Terms{MinuteRate: 12, UnlockFee: 50}
	start := time.Unix(0, 250_000_000)
	tests := []struct {
		elapsed time.Duration
		want    int64
	}{
		{4999 * time.Millisecond, 0},
		{5 * time.Second, 51},         // 50 + floor(5×12/60)
		{5999 * time.Millisecond, 51}, // seconds are floored
		{9 * time.Second, 51},         // 50 + floor(108/60)
		{10 * time.Second, 52},
		{time.Minute, 62},
	}
	for _, tt := range tests {
		if got, err := tripCost(terms, start, start.Add(tt.elapsed)); err != nil || got.Cmp(big.NewInt(tt.want)) != 0 {
			t.Errorf("tripCost(%s) = %v, %v, want %d", tt.elapsed, got, err, tt.want)
		}
	}
}

func TestTripCostWithLargeRate(t *testing.T) {
	start := time.Unix(0, 0)
	rate := int64(1) << 62
	got, err := tripCost(offer.Terms{MinuteRate: rate}, start, start.Add(5*time.Second))
	if want := rate / 12; err != nil || got.Cmp(big.NewInt(want)) != 0 {
		t.Fatalf("cost = %v, %v, want %d", got, err, want)
	}
	for _, test := range []struct {
		terms   offer.Terms
		elapsed time.Duration
		want    string
	}{
		{offer.Terms{MinuteRate: math.MaxInt64, UnlockFee: 1}, time.Minute, "9223372036854775808"},
		{offer.Terms{MinuteRate: math.MaxInt64}, 3 * time.Minute, "27670116110564327421"},
		{offer.Terms{MinuteRate: math.MaxInt64}, time.Hour, "553402322211286548420"},
	} {
		got, err := tripCost(test.terms, start, start.Add(test.elapsed))
		if err != nil || got.String() != test.want {
			t.Fatalf("cost with %+v after %v = %v, %v, want %s", test.terms, test.elapsed, got, err, test.want)
		}
	}
}

func TestTripCostBeyondTimeDuration(t *testing.T) {
	start := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	got, err := tripCost(offer.Terms{MinuteRate: 60}, start, start.AddDate(400, 0, 0))
	if err != nil || got.Cmp(big.NewInt(146097*86400)) != 0 {
		t.Fatalf("400-year cost = %v, %v", got, err)
	}
}

func TestFinishOutcome(t *testing.T) {
	start := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	terms := offer.Terms{MinuteRate: 60, UnlockFee: 10}
	if status, cost, err := finishOutcome(terms, start, start.Add(4*time.Second)); err != nil || status != Cancelled || cost.Sign() != 0 {
		t.Fatalf("short trip = %s %v, %v", status, cost, err)
	}
	if status, cost, err := finishOutcome(terms, start, start.Add(5*time.Second)); err != nil || status != Completed || cost.Cmp(big.NewInt(15)) != 0 {
		t.Fatalf("billable trip = %s %v, %v", status, cost, err)
	}
}

func TestOrderCostAt(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	started := now.Add(-time.Minute)
	order := trip{state: state{status: Starting}, terms: offer.Terms{MinuteRate: 60, UnlockFee: 10}}
	if cost, err := order.costAt(now); err != nil || cost != nil {
		t.Fatalf("STARTING cost = %v, %v", cost, err)
	}
	order.status, order.startedAt = Active, &started
	if cost, err := order.costAt(now); err != nil || cost == nil || cost.Cmp(big.NewInt(70)) != 0 {
		t.Fatalf("ACTIVE cost = %v, %v", cost, err)
	}
	order.status, order.cost = Completed, big.NewInt(5)
	if cost, err := order.costAt(now.Add(time.Hour)); err != nil || cost == nil || cost.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("COMPLETED cost = %v, %v", cost, err)
	}
}

func TestStartFailureReason(t *testing.T) {
	o := trip{state: state{status: StartFailed}}
	if got := o.startFailureReason(task.MaxAttemptsExceeded); got != task.DeadlineExceeded {
		t.Fatalf("reason for a start already closed by the API = %q", got)
	}
}
