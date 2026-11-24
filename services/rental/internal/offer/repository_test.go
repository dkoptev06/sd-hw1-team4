package offer

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/tests"
)

func TestCreateOfferPersists(t *testing.T) {
	store := tests.NewDB(t)
	f := newOfferFixture()
	repo := repository{db: store}
	f.service.repo = repo
	ctx := t.Context()

	first, created, err := f.service.Create(ctx, "s", "u", "key")
	if err != nil || !created {
		t.Fatalf("create: %+v created=%t err=%v", first, created, err)
	}
	stored, err := repo.findByKey(ctx, "key")
	if err != nil || stored.id != first.ID || stored.status != available ||
		stored.expiresAt.Sub(stored.createdAt) != 2*time.Minute {
		t.Fatalf("stored offer: %+v err=%v", stored, err)
	}
	replay, created, err := f.service.Create(ctx, "s", "u", "key")
	if err != nil || created || replay.ID != first.ID || !replay.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("replay: %+v created=%t err=%v", replay, created, err)
	}
	duplicate, inserted, err := repo.insert(ctx, stored.quote, time.Minute)
	if err != nil || inserted || duplicate.id != uuid.Nil {
		t.Fatalf("duplicate key insert: inserted=%t err=%v", inserted, err)
	}
}

func TestOfferRowRestoresOnlyValidModels(t *testing.T) {
	now := time.Now()
	valid := offerRow{
		id: uuid.New(), idempotencyID: "key", userID: "user", scooterID: "scooter", zoneID: "zone",
		charge: 20, minuteRate: 10, unlockFee: 50, deposit: 300, status: "AVAILABLE",
		createdAt: now, expiresAt: now.Add(time.Minute),
	}
	if o, err := valid.model(); err != nil || o.id != valid.id || !o.createdAt.Equal(now) {
		t.Fatalf("restore offer: %+v, %v", o, err)
	}
	for name, change := range map[string]func(*offerRow){
		"missing key":    func(r *offerRow) { r.idempotencyID = " " },
		"invalid charge": func(r *offerRow) { r.charge = 101 },
		"negative price": func(r *offerRow) { r.minuteRate = -1 },
		"unknown status": func(r *offerRow) { r.status = "EXPIRED" },
		"missing expiry": func(r *offerRow) { r.expiresAt = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			row := valid
			change(&row)
			if _, err := row.model(); err == nil {
				t.Fatalf("restored invalid row: %+v", row)
			}
		})
	}
}
