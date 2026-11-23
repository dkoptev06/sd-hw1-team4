package payment

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// RegisterRoutes installs the Payment API used by Rental.
func RegisterRoutes(router *echo.Echo) {
	router.POST("/payments/hold", hold)
	router.POST("/payments/settle", settle)
	router.POST("/payments/void", void)
	router.GET("/payment/:id", getPayment)
}

func hold(c *echo.Context) error {
	// TODO: Apply the idempotent HOLD command.
	return notImplemented(c)
}

func settle(c *echo.Context) error {
	// TODO: Apply the idempotent SETTLE command.
	return notImplemented(c)
}

func void(c *echo.Context) error {
	// TODO: Apply the idempotent VOID command.
	return notImplemented(c)
}

func getPayment(c *echo.Context) error {
	// TODO: Read the state of the payment cycle by operation_id.
	return notImplemented(c)
}

func notImplemented(c *echo.Context) error {
	return c.JSON(http.StatusNotImplemented, ErrorResponse{ErrorCode: "NOT_IMPLEMENTED"})
}
