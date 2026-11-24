package domain

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
)

func coefficients(t *testing.T, surge, discount string) Coefficients {
	t.Helper()
	s, err := ParseCoefficient(surge)
	if err != nil {
		t.Fatal(err)
	}
	d, err := ParseCoefficient(discount)
	if err != nil {
		t.Fatal(err)
	}
	return Coefficients{Surge: s, LowChargeDiscount: d}
}

func TestParseCoefficient(t *testing.T) {
	for _, bad := range []string{"", "0", "-1", "3/4", "NaN", "abc"} {
		if _, err := ParseCoefficient(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if r, err := ParseCoefficient("1.25"); err != nil || r.FloatString(2) != "1.25" {
		t.Fatalf("1.25 parsed as %v, %v", r, err)
	}
}

func TestPrice(t *testing.T) {
	tariff := Tariff{ZoneID: "zone", PricePerMinute: 10, PriceUnlock: 50, DefaultDeposit: 300}
	coeffs := coefficients(t, "1.25", "0.8")
	tests := []struct {
		name   string
		charge int
		user   UserProfile
		want   Terms
	}{
		{"regular", 28, UserProfile{}, Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 300}},
		{"low charge", 27, UserProfile{}, Terms{MinuteRate: 9, UnlockFee: 50, Deposit: 300}},
		{"subscription", 50, UserProfile{HasSubscription: true}, Terms{MinuteRate: 12, UnlockFee: 0, Deposit: 300}},
		{"trusted", 50, UserProfile{Trusted: true}, Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 0}},
		{"fallback is greedy", 50, FallbackProfile("u"), Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 300}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Price(tariff, tt.user, Scooter{ID: "s", ZoneID: "zone", Charge: tt.charge}, coeffs)
			if err != nil || got != tt.want {
				t.Fatalf("Price = %+v, %v, want %+v", got, err, tt.want)
			}
		})
	}
}

func TestPriceIsExact(t *testing.T) {
	// 100 × 1.15 is 114.99999999999999 in float64; the exact product is 115.
	got, err := Price(Tariff{ZoneID: "z", PricePerMinute: 100}, UserProfile{}, Scooter{Charge: 50}, coefficients(t, "1.15", "0.8"))
	if err != nil || got.MinuteRate != 115 {
		t.Fatalf("minute rate = %d, %v", got.MinuteRate, err)
	}
}

func TestPriceWithLargeRate(t *testing.T) {
	tariff := Tariff{ZoneID: "zone", PricePerMinute: 1 << 62}
	coeffs := coefficients(t, "4", "0.25")
	if _, err := Price(tariff, UserProfile{}, Scooter{Charge: 28}, coeffs); err == nil {
		t.Fatal("expected surge to exceed the supported amount")
	}
	got, err := Price(tariff, UserProfile{}, Scooter{Charge: 27}, coeffs)
	if err != nil || got.MinuteRate != tariff.PricePerMinute {
		t.Fatalf("discounted rate = %d, %v, want %d", got.MinuteRate, err, tariff.PricePerMinute)
	}
	if _, err := Price(tariff, UserProfile{}, Scooter{Charge: 27}, coefficients(t, "1", "4")); err == nil {
		t.Fatal("expected the second multiplication to exceed the supported amount")
	}
}

func TestTripCost(t *testing.T) {
	terms := Terms{MinuteRate: 12, UnlockFee: 50}
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
		if got, err := TripCost(terms, start, start.Add(tt.elapsed)); err != nil || got.Cmp(big.NewInt(tt.want)) != 0 {
			t.Errorf("TripCost(%s) = %v, %v, want %d", tt.elapsed, got, err, tt.want)
		}
	}
}

func TestTripCostWithLargeRate(t *testing.T) {
	start := time.Unix(0, 0)
	rate := int64(1) << 62
	got, err := TripCost(Terms{MinuteRate: rate}, start, start.Add(5*time.Second))
	if want := rate / 12; err != nil || got.Cmp(big.NewInt(want)) != 0 {
		t.Fatalf("cost = %v, %v, want %d", got, err, want)
	}
	for _, test := range []struct {
		terms   Terms
		elapsed time.Duration
		want    string
	}{
		{Terms{MinuteRate: math.MaxInt64, UnlockFee: 1}, time.Minute, "9223372036854775808"},
		{Terms{MinuteRate: math.MaxInt64}, 3 * time.Minute, "27670116110564327421"},
		{Terms{MinuteRate: math.MaxInt64}, time.Hour, "553402322211286548420"},
	} {
		got, err := TripCost(test.terms, start, start.Add(test.elapsed))
		if err != nil || got.String() != test.want {
			t.Fatalf("cost with %+v after %v = %v, %v, want %s", test.terms, test.elapsed, got, err, test.want)
		}
	}
}

func TestTripCostBeyondTimeDuration(t *testing.T) {
	start := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	got, err := TripCost(Terms{MinuteRate: 60}, start, start.AddDate(400, 0, 0))
	if err != nil || got.Cmp(big.NewInt(146097*86400)) != 0 {
		t.Fatalf("400-year cost = %v, %v", got, err)
	}
}

func TestFinishOutcome(t *testing.T) {
	start := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	terms := Terms{MinuteRate: 60, UnlockFee: 10}
	if status, cost, err := FinishOutcome(terms, start, start.Add(4*time.Second)); err != nil || status != OrderCancelled || cost.Sign() != 0 {
		t.Fatalf("short trip = %s %v, %v", status, cost, err)
	}
	if status, cost, err := FinishOutcome(terms, start, start.Add(5*time.Second)); err != nil || status != OrderCompleted || cost.Cmp(big.NewInt(15)) != 0 {
		t.Fatalf("billable trip = %s %v, %v", status, cost, err)
	}
}

func TestOrderCostAt(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	started := now.Add(-time.Minute)
	order := Order{Status: OrderStarting, Terms: Terms{MinuteRate: 60, UnlockFee: 10}}
	if cost, err := order.CostAt(now); err != nil || cost != nil {
		t.Fatalf("STARTING cost = %v, %v", cost, err)
	}
	order.Status, order.StartedAt = OrderActive, &started
	if cost, err := order.CostAt(now); err != nil || cost == nil || cost.Cmp(big.NewInt(70)) != 0 {
		t.Fatalf("ACTIVE cost = %v, %v", cost, err)
	}
	order.Status, order.Cost = OrderCompleted, big.NewInt(5)
	if cost, err := order.CostAt(now.Add(time.Hour)); err != nil || cost == nil || cost.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("COMPLETED cost = %v, %v", cost, err)
	}
}

func TestOfferCheckStart(t *testing.T) {
	expires := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	offer := Offer{Status: OfferAvailable, ExpiresAt: expires, Scooter: Scooter{ID: "s", ZoneID: "zone", Charge: 28}}
	before := expires.Add(-time.Second)
	tests := []struct {
		name    string
		status  OfferStatus
		now     time.Time
		current Scooter
		want    error
	}{
		{"same category", OfferAvailable, before, Scooter{ID: "s", ZoneID: "zone", Charge: 90}, nil},
		{"crossed threshold", OfferAvailable, before, Scooter{ID: "s", ZoneID: "zone", Charge: 27}, ErrOfferChanged},
		{"other zone", OfferAvailable, before, Scooter{ID: "s", ZoneID: "other", Charge: 28}, ErrOfferChanged},
		{"expiry boundary", OfferAvailable, expires, Scooter{ID: "s", ZoneID: "zone", Charge: 28}, ErrOfferExpired},
		{"consumed", OfferConsumed, before, Scooter{ID: "s", ZoneID: "zone", Charge: 28}, ErrOfferExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := offer
			o.Status = tt.status
			if err := o.CheckStart(tt.now, tt.current); !errors.Is(err, tt.want) && !(err == nil && tt.want == nil) {
				t.Fatalf("CheckStart = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestRetention(t *testing.T) {
	zones := map[string]int{"zone-2": 30}
	r, err := NewRetention(365, zones)
	if err != nil {
		t.Fatal(err)
	}
	zones["zone-2"] = 1
	closed := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	if got := r.DeleteAfter(closed, "zone-2"); !got.Equal(closed.AddDate(0, 0, 30)) {
		t.Fatalf("zone retention = %s", got)
	}
	if got := r.DeleteAfter(closed, "zone-1"); !got.Equal(closed.AddDate(0, 0, 365)) {
		t.Fatalf("default retention = %s", got)
	}
	if _, err := NewRetention(0, nil); err == nil {
		t.Fatal("accepted zero default")
	}
	if _, err := NewRetention(365, map[string]int{"z": 0}); err == nil {
		t.Fatal("accepted zero zone retention")
	}
}

func TestTaskRules(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	claim := uuid.New()
	live, expired := now.Add(time.Second), now.Add(-time.Second)
	future, past := now.Add(time.Minute), now.Add(-time.Second)
	task := func(attempts int, lease *time.Time, deadline *time.Time) Task {
		t := Task{Kind: TaskStart, Status: TaskPending, AttemptCount: attempts, Deadline: deadline}
		if lease != nil {
			t.ClaimID, t.LeaseUntil = &claim, lease
		}
		return t
	}

	if !task(1, &live, &future).Owns(claim, now) {
		t.Fatal("live claim must own the task")
	}
	if task(1, &expired, &future).Owns(claim, now) || task(1, &live, &past).Owns(claim, now) ||
		task(1, &live, &future).Owns(uuid.New(), now) {
		t.Fatal("stale claim, deadline or foreign claim must not own the task")
	}

	const maxAttempts = 3
	tests := []struct {
		name  string
		task  Task
		order OrderStatus
		want  Reason
	}{
		{"attempts left", task(2, nil, &future), OrderStarting, ""},
		{"last attempt in flight", task(3, &live, &future), OrderStarting, ""},
		{"last lease expired", task(3, &expired, &future), OrderStarting, ReasonMaxAttemptsExceeded},
		{"closed by API", task(3, nil, &future), OrderStartFailed, ReasonDeadlineExceeded},
		{"deadline beats live lease", task(1, &live, &past), OrderStarting, ReasonDeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StartFailureReason(tt.task, tt.order, now, maxAttempts); got != tt.want {
				t.Fatalf("reason = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	low := func() float64 { return 0 }
	high := func() float64 { return 0.999999 }
	base, maxDelay := time.Second, 10*time.Second
	if got := Backoff(1, base, maxDelay, low); got != 500*time.Millisecond {
		t.Fatalf("attempt 1 low = %s", got)
	}
	if got := Backoff(3, base, maxDelay, high); got < 3900*time.Millisecond || got > 4*time.Second {
		t.Fatalf("attempt 3 high = %s", got)
	}
	if got := Backoff(50, base, maxDelay, high); got > maxDelay {
		t.Fatalf("cap exceeded: %s", got)
	}
}
