package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotValidation(t *testing.T) {
	overrides := map[string]int{"zone": 30}
	retention, err := NewRetention(365, overrides)
	if err != nil {
		t.Fatal(err)
	}
	overrides["zone"] = 1
	closed := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if got := retention.DeleteAfter(closed, "zone"); !got.Equal(closed.AddDate(0, 0, 30)) {
		t.Fatalf("retention changed after construction: %s", got)
	}
	if _, err := NewRetention(365, map[string]int{"zone": 0}); err == nil {
		t.Fatal("accepted a nonpositive zone override")
	}
	for _, number := range []json.Number{"0", "-1", "3/4", "NaN"} {
		if _, err := New(number, "0.8", retention, 10); err == nil {
			t.Fatalf("accepted invalid price coefficient %q", number)
		}
	}
	defaults := Default()
	if defaults.Surge() != "1.25" || defaults.LowChargeDiscount() != "0.8" || defaults.TariffZoneCacheTTLMinutes() != 10 ||
		!defaults.Retention().DeleteAfter(closed, "zone").Equal(closed.AddDate(0, 0, 365)) {
		t.Fatal("unexpected cold-start defaults")
	}
}
