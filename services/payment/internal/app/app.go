// Package app serves the Payment stub over HTTP.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dkoptev06/sd-hw1-team4/services/payment/internal/ledger"
)

// Run serves HTTP until ctx is done.
func Run(ctx context.Context) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	addr := os.Getenv("PAYMENT_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	slog.Info("serving Payment stub", "addr", addr)
	return start(ctx, addr, NewRouter(ledger.New(time.Now)))
}

type holdRequest struct {
	OperationID   string    `json:"operation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	HoldExpiresAt time.Time `json:"hold_expires_at"`
}

type settleRequest struct {
	OperationID string `json:"operation_id"`
	UserID      string `json:"user_id"`
	TotalAmount int64  `json:"total_amount"`
}

type voidRequest struct {
	OperationID string `json:"operation_id"`
}

type statusResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	ErrorCode string `json:"error_code"`
}

// NewRouter exposes the ledger through the agreed Payment API.
func NewRouter(l *ledger.Ledger) *echo.Echo {
	e := echo.New()
	e.Logger = slog.Default()
	e.HTTPErrorHandler = func(c *echo.Context, err error) {
		status := echo.StatusCode(err)
		if status == 0 {
			status = http.StatusInternalServerError
		}
		_ = c.JSON(status, errorResponse{ErrorCode: strings.ToUpper(strings.ReplaceAll(http.StatusText(status), " ", "_"))})
	}
	e.POST("/payments/hold", command(func(req holdRequest) (string, error) {
		return l.Hold(req.OperationID, req.UserID, req.Amount, req.HoldExpiresAt)
	}))
	e.POST("/payments/settle", command(func(req settleRequest) (string, error) {
		return l.Settle(req.OperationID, req.UserID, req.TotalAmount)
	}))
	e.POST("/payments/void", command(func(req voidRequest) (string, error) {
		return l.Void(req.OperationID)
	}))
	e.GET("/payment/:id", func(c *echo.Context) error {
		status, err := l.Status(c.Param("id"))
		return reply(c, status, err)
	})
	e.GET("/healthz", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	return e
}

// command decodes a JSON body regardless of Content-Type and runs a ledger command.
func command[T any](run func(T) (string, error)) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var req T
		body := http.MaxBytesReader(c.Response(), c.Request().Body, 64<<10)
		if err := json.NewDecoder(body).Decode(&req); err != nil {
			return reply(c, "", ledger.ErrInvalid)
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
	case errors.Is(err, ledger.ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, ledger.ErrNotFound):
		code = http.StatusNotFound
	}
	return c.JSON(code, errorResponse{ErrorCode: err.Error()})
}

func start(ctx context.Context, addr string, e *echo.Echo) error {
	var shutdownErr error
	err := echo.StartConfig{
		Address:         addr,
		HideBanner:      true,
		GracefulTimeout: 5 * time.Second,
		BeforeServeFunc: func(s *http.Server) error {
			s.ReadTimeout = 0
			s.ReadHeaderTimeout = 5 * time.Second
			return nil
		},
		OnShutdownError: func(err error) { shutdownErr = err },
	}.Start(ctx, e)
	return errors.Join(err, shutdownErr)
}
