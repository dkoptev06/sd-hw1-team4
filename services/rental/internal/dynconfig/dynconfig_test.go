package dynconfig

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
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
		`{"price_coeff_settings":{"surge":0,"low_charge_discount":0.8},"default_retention_days":365}`,
		`{"price_coeff_settings":{"surge":1.2},"default_retention_days":365}`,
		`{"price_coeff_settings":{"surge":1.2,"low_charge_discount":0.8},"default_retention_days":0}`,
		`{"price_coeff_settings":{"surge":1.2,"low_charge_discount":0.8},"default_retention_days":365,"tariff_zone_cache_ttl_minutes":0}`,
	} {
		if _, err := Parse(raw(t, body)); err == nil {
			t.Errorf("accepted %s", body)
		}
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
	next = raw(t, `{"price_coeff_settings":{"surge":1.5,"low_charge_discount":0.8},"default_retention_days":30}`)
	if err := provider.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	next = raw(t, `{"price_coeff_settings":{"surge":-1,"low_charge_discount":0.8},"default_retention_days":30}`)
	if err := provider.Refresh(context.Background()); err == nil {
		t.Fatal("invalid config accepted")
	}
	if got := provider.Current(); got.Coefficients.Surge.FloatString(1) != "1.5" || got.Retention.DefaultDays() != 30 {
		t.Fatalf("last valid config lost: %+v", got)
	}
}
