package dynconfig

import (
	"testing"
	"time"
)

func TestParseCoefficient(t *testing.T) {
	for _, bad := range []string{"", "0", "-1", "3/4", "NaN", "abc"} {
		if _, err := parseCoefficient(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if r, err := parseCoefficient("1.25"); err != nil || r.FloatString(2) != "1.25" {
		t.Fatalf("1.25 parsed as %v, %v", r, err)
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
