package userprofile

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/platform/httpx"
)

// RegisterRoutes installs the userprofile endpoint.
func RegisterRoutes(router *echo.Echo) {
	router.GET("/user-profile/:id", getUserProfile)
}

func getUserProfile(c *echo.Context) error {
	// TODO: Read the user profile from Source state.
	return httpx.NotImplemented(c)
}
