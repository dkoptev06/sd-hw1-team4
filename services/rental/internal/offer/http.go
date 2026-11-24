package offer

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
)

func (s *Service) RegisterRoutes(router *echo.Echo, middleware ...echo.MiddlewareFunc) {
	router.POST("/offers", s.createOffer, middleware...)
}

func (s *Service) createOffer(c *echo.Context) error {
	var req createOfferRequest
	if err := httpx.DecodeJSON(c, &req); err != nil {
		return err
	}
	o, created, err := s.Create(c.Request().Context(), req.ScooterID, req.UserID, req.IdempotencyID)
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	return c.JSON(status, offerResponse{
		OfferID: o.ID.String(), MinuteRate: o.Terms.MinuteRate, UnlockFee: o.Terms.UnlockFee,
		Deposit: o.Terms.Deposit, ExpiresAt: o.ExpiresAt,
	})
}
