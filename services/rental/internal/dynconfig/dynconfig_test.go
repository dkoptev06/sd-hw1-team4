package dynconfig

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
)

type fetcherFunc func(context.Context) (source.Configs, error)

func (f fetcherFunc) Configs(ctx context.Context) (source.Configs, error) { return f(ctx) }

func raw(t *testing.T, body string) source.Configs {
	t.Helper()
	var c source.Configs
	if err := json.Unmarshal([]byte(body), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParse(t *testing.T) {
	snapshot, err := Parse(raw(t, `{"price_coeff_settings":{"surge":1.5,"low_charge_discount":0.8},
		"default_retention_days":365,"retention_days_by_zone":{"zone-2":30},"tariff_zone_cache_ttl_minutes":5}`))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Coefficients.Surge.FloatString(1) != "1.5" || snapshot.TariffTTL != 5*time.Minute ||
		snapshot.Retention.DaysByZone()["zone-2"] != 30 {
		t.Fatalf("unexpected snapshot %+v", snapshot)
	}
	for _, body := range []string{
		`{"price_coeff_settings":{"surge":0,"low_charge_discount":0.8},"default_retention_days":365,"retention_days_by_zone":{}}`,
		`{"price_coeff_settings":{"surge":1.2},"default_retention_days":365,"retention_days_by_zone":{}}`,
		`{"price_coeff_settings":{"surge":1.2,"low_charge_discount":0.8},"default_retention_days":0,"retention_days_by_zone":{}}`,
		`{"price_coeff_settings":{"surge":1.2,"low_charge_discount":0.8},"default_retention_days":365,"retention_days_by_zone":{},"tariff_zone_cache_ttl_minutes":0}`,
	} {
		if _, err := Parse(raw(t, body)); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
}

func TestParseTariffTTLBoundary(t *testing.T) {
	cfg := defaultConfigs()
	// This is the largest whole number of minutes representable by time.Duration.
	minutes := 153_722_867
	cfg.TariffZoneCacheTTLMinutes = &minutes
	snapshot, err := Parse(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Duration(minutes) * time.Minute; snapshot.TariffTTL != want {
		t.Fatalf("TariffTTL = %v, want %v", snapshot.TariffTTL, want)
	}

	minutes++
	if _, err := Parse(cfg); err == nil {
		t.Fatalf("accepted overflowing tariff TTL of %d minutes", minutes)
	}
}

func TestProviderKeepsLastValid(t *testing.T) {
	var next source.Configs
	var fail error
	provider := NewProvider(fetcherFunc(func(context.Context) (source.Configs, error) { return next, fail }),
		time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))

	defaults := provider.Current()
	if defaults.Coefficients.Surge.FloatString(2) != "1.25" || defaults.TariffTTL != 10*time.Minute ||
		defaults.Retention.DefaultDays() != 365 {
		t.Fatalf("unexpected defaults %+v", defaults)
	}

	fail = errors.New("down")
	if err := provider.Refresh(context.Background()); err == nil {
		t.Fatal("refresh error not reported")
	}
	if provider.Current().Coefficients.Surge.FloatString(2) != "1.25" {
		t.Fatal("cold start must keep the defaults")
	}

	fail = nil
	overflowing := raw(t, `{"price_coeff_settings":{"surge":2,"low_charge_discount":0.8},
		"default_retention_days":10,"retention_days_by_zone":{},"tariff_zone_cache_ttl_minutes":153722868}`)
	next = overflowing
	if err := provider.Refresh(context.Background()); err == nil {
		t.Fatal("overflowing tariff TTL accepted at cold start")
	}
	if got := provider.Current(); got.TariffTTL != defaults.TariffTTL ||
		got.Coefficients.Surge.Cmp(defaults.Coefficients.Surge) != 0 ||
		got.Retention.DefaultDays() != defaults.Retention.DefaultDays() {
		t.Fatalf("overflowing tariff TTL replaced defaults: %+v", got)
	}

	valid := raw(t, `{"price_coeff_settings":{"surge":1.5,"low_charge_discount":0.8},
		"default_retention_days":365,"retention_days_by_zone":{"zone-2":30},"tariff_zone_cache_ttl_minutes":7}`)
	next = valid
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []source.Configs{
		raw(t, `{"price_coeff_settings":{"surge":-1,"low_charge_discount":0.8},"default_retention_days":30,"retention_days_by_zone":{}}`),
		raw(t, `{"price_coeff_settings":{"surge":2,"low_charge_discount":0.5},"default_retention_days":10,"retention_days_by_zone":null}`),
		raw(t, `{"price_coeff_settings":{"surge":2,"low_charge_discount":0.5},"default_retention_days":10}`),
		overflowing,
	} {
		next = invalid
		if err := provider.Refresh(context.Background()); err == nil {
			t.Fatalf("invalid config accepted: %+v", invalid)
		}
		if got := provider.Current(); got.Coefficients.Surge.FloatString(1) != "1.5" ||
			got.Coefficients.LowChargeDiscount.FloatString(1) != "0.8" || got.Retention.DefaultDays() != 365 ||
			got.Retention.DaysByZone()["zone-2"] != 30 || got.TariffTTL != 7*time.Minute {
			t.Fatalf("last valid config lost: %+v", got)
		}
	}
	next = valid
	next.RetentionDaysByZone = map[string]int{}
	if err := provider.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if zones := provider.Current().Retention.DaysByZone(); len(zones) != 0 {
		t.Fatalf("empty object should clear zone overrides, got %v", zones)
	}
}
