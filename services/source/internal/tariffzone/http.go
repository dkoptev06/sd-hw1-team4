package tariffzone

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/platform/httpx"
)

// RegisterRoutes installs the tariffzone endpoint.
func RegisterRoutes(router *echo.Echo) {
	router.GET("/tariff-zone/:id", getTariffZone)
}

func getTariffZone(c *echo.Context) error {
	// TODO: Read the tariff zone from Source state.
	return httpx.NotImplemented(c)
}
