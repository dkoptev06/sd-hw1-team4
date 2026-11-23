package order

import (
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
)

// RegisterRoutes installs the client-facing order API.
func RegisterRoutes(router *echo.Echo) {
	router.POST("/orders", handleStartOrder)
	router.GET("/orders/:order_id", handleGetOrder)
	router.POST("/orders/:order_id/finish", handleFinishOrder)
}

func handleStartOrder(c *echo.Context) error {
	// TODO: Decode the offer ID, call order.Service.StartOrder and map the result to HTTP.
	return httpx.NotImplemented(c)
}

func handleGetOrder(c *echo.Context) error {
	// TODO: Read the current or archived order and map its view to OrderResponse.
	return httpx.NotImplemented(c)
}

func handleFinishOrder(c *echo.Context) error {
	// TODO: Call the order service and return the persisted final result.
	return httpx.NotImplemented(c)
}
