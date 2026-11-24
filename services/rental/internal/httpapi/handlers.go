package httpapi

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/rental"
)

// Offers is the offer use case.
type Offers interface {
	Create(ctx context.Context, req rental.CreateOfferRequest) (domain.Offer, bool, error)
}

// Orders is the order use case.
type Orders interface {
	Start(ctx context.Context, offerID uuid.UUID) (rental.StartResult, error)
	Get(ctx context.Context, orderID uuid.UUID) (rental.OrderView, error)
	Finish(ctx context.Context, orderID uuid.UUID) (domain.Order, error)
}

type API struct {
	offers Offers
	orders Orders
}

func NewAPI(offers Offers, orders Orders) *API {
	return &API{offers: offers, orders: orders}
}

type createOfferRequest struct {
	ScooterID     string `json:"scooter_id"`
	UserID        string `json:"user_id"`
	IdempotencyID string `json:"idempotency_id"`
}

type offerResponse struct {
	OfferID    string    `json:"offer_id"`
	MinuteRate int64     `json:"minute_rate"`
	UnlockFee  int64     `json:"unlock_fee"`
	Deposit    int64     `json:"deposit"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func (a *API) createOffer(c *echo.Context) error {
	var req createOfferRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	offer, created, err := a.offers.Create(c.Request().Context(), rental.CreateOfferRequest(req))
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	return c.JSON(status, offerResponse{
		OfferID: offer.ID.String(), MinuteRate: offer.Terms.MinuteRate, UnlockFee: offer.Terms.UnlockFee,
		Deposit: offer.Terms.Deposit, ExpiresAt: offer.ExpiresAt,
	})
}

type startOrderRequest struct {
	OfferID string `json:"offer_id"`
}

type startOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func (a *API) startOrder(c *echo.Context) error {
	var req startOrderRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	if req.OfferID == "" {
		return domain.Wrap(domain.ErrInvalidRequest, "offer_id is required")
	}
	offerID, err := parseID(req.OfferID)
	if err != nil {
		return err
	}
	result, err := a.orders.Start(c.Request().Context(), offerID)
	if err != nil {
		return err
	}
	status := http.StatusAccepted
	if result.Replayed {
		status = http.StatusOK
	}
	return c.JSON(status, startOrderResponse{OrderID: result.OrderID.String(), Status: string(result.Status)})
}

type orderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	Cost       *big.Int   `json:"cost"`
	StartedAt  *time.Time `json:"started_at"`
	FinishTime *time.Time `json:"finish_time"`
}

func (a *API) getOrder(c *echo.Context) error {
	started := time.Now()
	orderID, err := parseID(c.Param("order_id"))
	if err != nil {
		return err
	}
	view, err := a.orders.Get(c.Request().Context(), orderID)
	if err != nil {
		return err
	}
	storage := "current"
	if view.Archived {
		storage = "history"
	}
	metrics.OrderReads.WithLabelValues(storage).Observe(time.Since(started).Seconds())
	o := view.Order
	return c.JSON(http.StatusOK, orderResponse{
		OrderID: o.ID.String(), Status: string(o.Status), Cost: view.Cost, StartedAt: o.StartedAt, FinishTime: o.FinishTime,
	})
}

type finishOrderResponse struct {
	OrderID    string     `json:"order_id"`
	Status     string     `json:"status"`
	FinishTime *time.Time `json:"finish_time"`
	Cost       *big.Int   `json:"cost"`
}

func (a *API) finishOrder(c *echo.Context) error {
	orderID, err := parseID(c.Param("order_id"))
	if err != nil {
		return err
	}
	o, err := a.orders.Finish(c.Request().Context(), orderID)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, finishOrderResponse{
		OrderID: o.ID.String(), Status: string(o.Status), FinishTime: o.FinishTime, Cost: o.Cost,
	})
}

const maxBodyBytes = 64 << 10

// decode reads a JSON body regardless of Content-Type, so plain `curl -d` and load
// generators that omit the header are accepted.
func decode(c *echo.Context, dst any) error {
	body := http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes)
	if err := json.NewDecoder(body).Decode(dst); err != nil {
		return domain.Wrap(domain.ErrInvalidRequest, "invalid JSON body: %v", err)
	}
	return nil
}

// parseID treats a malformed ID as unknown: no such offer or order can exist.
func parseID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, domain.Wrap(domain.ErrNotFound, "malformed ID %q", value)
	}
	return id, nil
}
