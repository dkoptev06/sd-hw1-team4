package offer

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
)

// RegisterRoutes installs the client-facing offer API.
func RegisterRoutes(router *echo.Echo) {
	router.POST("/offers", handleCreateOffer)
}

func handleCreateOffer(c *echo.Context) error {
	// TODO: Price and persist the quote, returning 201 for creation and 200 for a replay.
	return httpx.NotImplemented(c)
}
