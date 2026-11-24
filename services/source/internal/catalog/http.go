package catalog

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (c *Service) RegisterRoutes(router *echo.Echo) {
	router.GET("/scooter/:id", byID(c.scooter, scooterDTO))
	router.GET("/tariff-zone/:id", byID(c.tariffZone, tariffZoneDTO))
	router.GET("/user-profile/:id", byID(c.userProfile, userProfileDTO))
	router.GET("/configs", func(ctx *echo.Context) error {
		return ctx.JSON(http.StatusOK, configsDTO(c.config))
	})
}

func byID[T, D any](lookup func(string) (T, bool), response func(T) D) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		id := ctx.Param("id")
		if id == "" {
			return ctx.JSON(http.StatusBadRequest, errorBody{ErrorCode: "INVALID_REQUEST"})
		}
		value, ok := lookup(id)
		if !ok {
			return ctx.JSON(http.StatusNotFound, errorBody{ErrorCode: "NOT_FOUND"})
		}
		return ctx.JSON(http.StatusOK, response(value))
	}
}
