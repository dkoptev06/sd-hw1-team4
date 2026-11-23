package scooter

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/platform/httpx"
)

// RegisterRoutes installs the scooter endpoint.
func RegisterRoutes(router *echo.Echo) {
	router.GET("/scooter/:id", getScooter)
}

func getScooter(c *echo.Context) error {
	// TODO: Read the scooter and return its current charge and zone.
	return httpx.NotImplemented(c)
}
