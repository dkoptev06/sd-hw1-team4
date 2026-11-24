package ledger

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (s *Service) RegisterRoutes(e *echo.Echo) {
	e.POST("/payments/hold", command(func(req holdRequest) (string, error) {
		return s.Hold(req.OperationID, req.UserID, req.Amount, req.HoldExpiresAt)
	}))
	e.POST("/payments/settle", command(func(req settleRequest) (string, error) {
		return s.Settle(req.OperationID, req.UserID, req.TotalAmount)
	}))
	e.POST("/payments/void", command(func(req voidRequest) (string, error) {
		return s.Void(req.OperationID)
	}))
	e.GET("/payment/:id", func(c *echo.Context) error {
		status, err := s.Status(c.Param("id"))
		return reply(c, status, err)
	})
}

// command decodes a JSON body regardless of Content-Type and runs a ledger command.
func command[T any](run func(T) (string, error)) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var req T
		body := http.MaxBytesReader(c.Response(), c.Request().Body, 64<<10)
		if err := json.NewDecoder(body).Decode(&req); err != nil {
			return reply(c, "", errInvalid)
		}
		result, err := run(req)
		return reply(c, result, err)
	}
}

func reply(c *echo.Context, status string, err error) error {
	if err == nil {
		return c.JSON(http.StatusOK, statusResponse{Status: status})
	}
	code := http.StatusConflict
	switch {
	case errors.Is(err, errInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, errNotFound):
		code = http.StatusNotFound
	}
	return c.JSON(code, errorResponse{ErrorCode: err.Error()})
}
