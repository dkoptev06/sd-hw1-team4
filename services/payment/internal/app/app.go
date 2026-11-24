// Package app serves the Payment stub over HTTP.
package app

import (
	"context"
	"errors"
	"fmt"
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
	settleResult := os.Getenv("PAYMENT_SETTLE_RESULT")
	if settleResult != "" && settleResult != ledger.Success && settleResult != ledger.Failed {
		return fmt.Errorf("invalid PAYMENT_SETTLE_RESULT %q", settleResult)
	}
	slog.Info("serving Payment stub", "addr", addr)
	return start(ctx, addr, newRouter(ledger.NewService(time.Now, settleResult == ledger.Failed)))
}

type errorResponse struct {
	ErrorCode string `json:"error_code"`
}

func newRouter(l *ledger.Service) *echo.Echo {
	e := echo.New()
	e.Logger = slog.Default()
	e.HTTPErrorHandler = func(c *echo.Context, err error) {
		status := echo.StatusCode(err)
		if status == 0 {
			status = http.StatusInternalServerError
		}
		_ = c.JSON(status, errorResponse{ErrorCode: strings.ToUpper(strings.ReplaceAll(http.StatusText(status), " ", "_"))})
	}
	l.RegisterRoutes(e)
	e.GET("/healthz", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	return e
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
