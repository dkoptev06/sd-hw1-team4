package order

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
)

type orderOperations interface {
	Start(ctx context.Context, offerID uuid.UUID) (StartResult, error)
	Get(ctx context.Context, orderID uuid.UUID) (View, error)
	Finish(ctx context.Context, orderID uuid.UUID) (FinishReceipt, error)
}

type orderHandler struct {
	orders orderOperations
}

func (s *Service) RegisterRoutes(router *echo.Echo, middleware ...echo.MiddlewareFunc) {
	(&orderHandler{orders: s}).registerRoutes(router, middleware...)
}

func (a *orderHandler) registerRoutes(router *echo.Echo, middleware ...echo.MiddlewareFunc) {
	router.POST("/orders", a.startOrder, middleware...)
	router.GET("/orders/:order_id", a.getOrder, middleware...)
	router.POST("/orders/:order_id/finish", a.finishOrder, middleware...)
}

func (a *orderHandler) startOrder(c *echo.Context) error {
	var req startOrderRequest
	if err := httpx.DecodeJSON(c, &req); err != nil {
		return err
	}
	if req.OfferID == "" {
		return fmt.Errorf("%w: offer_id is required", ErrInvalidRequest)
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

func (a *orderHandler) getOrder(c *echo.Context) error {
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
	return c.JSON(http.StatusOK, orderResponse{
		OrderID: view.ID.String(), Status: string(view.Status), Cost: view.Cost, StartedAt: view.StartedAt, FinishTime: view.FinishTime,
	})
}

func (a *orderHandler) finishOrder(c *echo.Context) error {
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

// parseID treats a malformed ID as unknown: no such offer or order can exist.
func parseID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: malformed ID %q", ErrNotFound, value)
	}
	return id, nil
}
