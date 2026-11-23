package httpx

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

type errorResponse struct {
	ErrorCode string `json:"error_code"`
}

// NotImplemented writes the temporary response for an unfinished endpoint.
func NotImplemented(c *echo.Context) error {
	return c.JSON(http.StatusNotImplemented, errorResponse{ErrorCode: "NOT_IMPLEMENTED"})
}
