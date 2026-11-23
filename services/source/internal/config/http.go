package config

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/platform/httpx"
)

// RegisterRoutes installs the config endpoint.
func RegisterRoutes(router *echo.Echo) {
	router.GET("/configs", getConfigs)
}

func getConfigs(c *echo.Context) error {
	// TODO: Serve the current Source configuration.
	return httpx.NotImplemented(c)
}
