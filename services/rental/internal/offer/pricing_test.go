package offer

import (
	"encoding/json"
	"testing"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

func coefficients(t *testing.T, surge, discount string) dynconfig.Coefficients {
	t.Helper()
	var raw source.Configs
	raw.PriceCoeffSettings.Surge = json.Number(surge)
	raw.PriceCoeffSettings.LowChargeDiscount = json.Number(discount)
	raw.DefaultRetentionDays = 365
	raw.RetentionDaysByZone = map[string]int{}
	snapshot, err := dynconfig.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Coefficients
}

func TestPrice(t *testing.T) {
	tariff := source.Tariff{ZoneID: "zone", PricePerMinute: 10, PriceUnlock: 50, DefaultDeposit: 300}
	coeffs := coefficients(t, "1.25", "0.8")
	tests := []struct {
		name   string
		charge int
		user   source.UserProfile
		want   Terms
	}{
		{"regular", 28, source.UserProfile{}, Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 300}},
		{"low charge", 27, source.UserProfile{}, Terms{MinuteRate: 9, UnlockFee: 50, Deposit: 300}},
		{"subscription", 50, source.UserProfile{HasSubscription: true}, Terms{MinuteRate: 12, UnlockFee: 0, Deposit: 300}},
		{"trusted", 50, source.UserProfile{Trusted: true}, Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 0}},
		{"fallback is greedy", 50, fallbackProfile("u").UserProfile, Terms{MinuteRate: 12, UnlockFee: 50, Deposit: 300}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := price(tariff, tt.user, source.Scooter{ID: "s", ZoneID: "zone", Charge: tt.charge}, coeffs)
			if err != nil || got != tt.want {
				t.Fatalf("Price = %+v, %v, want %+v", got, err, tt.want)
			}
		})
	}
}

func TestPriceIsExact(t *testing.T) {
	// 100 × 1.15 is 114.99999999999999 in float64; the exact product is 115.
	got, err := price(source.Tariff{ZoneID: "z", PricePerMinute: 100}, source.UserProfile{}, source.Scooter{Charge: 50}, coefficients(t, "1.15", "0.8"))
	if err != nil || got.MinuteRate != 115 {
		t.Fatalf("minute rate = %d, %v", got.MinuteRate, err)
	}
}

func TestPriceWithLargeRate(t *testing.T) {
	tariff := source.Tariff{ZoneID: "zone", PricePerMinute: 1 << 62}
	coeffs := coefficients(t, "4", "0.25")
	if _, err := price(tariff, source.UserProfile{}, source.Scooter{Charge: 28}, coeffs); err == nil {
		t.Fatal("expected surge to exceed the supported amount")
	}
	got, err := price(tariff, source.UserProfile{}, source.Scooter{Charge: 27}, coeffs)
	if err != nil || got.MinuteRate != tariff.PricePerMinute {
		t.Fatalf("discounted rate = %d, %v, want %d", got.MinuteRate, err, tariff.PricePerMinute)
	}
	if _, err := price(tariff, source.UserProfile{}, source.Scooter{Charge: 27}, coefficients(t, "1", "4")); err == nil {
		t.Fatal("expected the second multiplication to exceed the supported amount")
	}
}
