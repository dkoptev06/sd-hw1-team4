package order

import (
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
)

// archiveCacheRecord preserves the Redis v1 format across API versions.
type archiveCacheRecord struct {
	Order     archivedOrderRecord
	ExpiresAt time.Time
}

type archivedOrderRecord struct {
	ID          uuid.UUID
	OfferID     uuid.UUID
	UserID      string
	ScooterID   string
	ZoneID      string
	Terms       offer.Terms
	Status      Status
	CreatedAt   time.Time
	StartedAt   *time.Time
	FinishTime  *time.Time
	ClosedAt    *time.Time
	DeleteAfter *time.Time
	Cost        *big.Int
}

func (e cachedEntry) record() archiveCacheRecord {
	o := e.Order
	return archiveCacheRecord{
		Order: archivedOrderRecord{
			ID: o.id, OfferID: o.offerID, UserID: o.userID, ScooterID: o.scooterID, ZoneID: o.zoneID,
			Terms: o.terms, Status: o.status, CreatedAt: o.createdAt, StartedAt: o.startedAt,
			FinishTime: o.finishTime, ClosedAt: o.closedAt, DeleteAfter: o.deleteAfter, Cost: o.cost,
		},
		ExpiresAt: e.ExpiresAt,
	}
}

func (r archiveCacheRecord) entry() (cachedEntry, error) {
	o := r.Order
	value, err := restoreTrip(o.ID, o.OfferID, o.UserID, o.ScooterID, o.ZoneID, o.Terms, state{
		status: o.Status, createdAt: o.CreatedAt, startedAt: o.StartedAt, finishTime: o.FinishTime,
		closedAt: o.ClosedAt, deleteAfter: o.DeleteAfter, cost: o.Cost,
	})
	if err != nil {
		return cachedEntry{}, err
	}
	return cachedEntry{Order: value, ExpiresAt: r.ExpiresAt}, nil
}
