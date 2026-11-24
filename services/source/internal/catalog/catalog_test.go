package catalog

import (
	"strconv"
	"testing"
)

func TestSeedCoversPricingCases(t *testing.T) {
	c, err := NewService(400, 12)
	if err != nil {
		t.Fatal(err)
	}
	zones, low, high := map[string]int{}, 0, 0
	for i := 1; i <= 400; i++ {
		s, _ := c.scooter("scooter-" + strconv.Itoa(i))
		zones[s.zoneID]++
		if s.charge < 28 {
			low++
		} else {
			high++
		}
	}
	if len(zones) != 4 || zones["zone-1"] != 100 || low == 0 || high == 0 {
		t.Fatalf("zones %v low %d high %d", zones, low, high)
	}
	combos := map[[2]bool]bool{}
	for i := 1; i <= 12; i++ {
		u, _ := c.userProfile("user-" + strconv.Itoa(i))
		combos[[2]bool{u.hasSubscription, u.trusted}] = true
	}
	if len(combos) != 4 {
		t.Fatalf("user flag combinations %v", combos)
	}
}
