// Package httpserver holds the Echo setup shared by all Rental processes.
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New creates an Echo instance logging through log.
func New(log *slog.Logger) *echo.Echo {
	e := echo.New()
	e.Logger = log
	return e
}

// RegisterOps installs /healthz, /readyz (checks ready) and /metrics.
func RegisterOps(e *echo.Echo, ready func(context.Context) error) {
	e.GET("/healthz", func(c *echo.Context) error {
		return c.NoContent(http.StatusNoContent)
	})
	e.GET("/readyz", func(c *echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			return c.NoContent(http.StatusServiceUnavailable)
		}
		return c.NoContent(http.StatusNoContent)
	})
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
}

// Start serves e on addr until ctx is done, then shuts down gracefully.
func Start(ctx context.Context, addr string, e *echo.Echo) error {
	var shutdownErr error
	err := echo.StartConfig{
		Address:         addr,
		HideBanner:      true,
		GracefulTimeout: 10 * time.Second,
		BeforeServeFunc: func(s *http.Server) error {
			s.ReadTimeout = 0
			s.ReadHeaderTimeout = 5 * time.Second
			s.IdleTimeout = 2 * time.Minute
			return nil
		},
		OnShutdownError: func(err error) { shutdownErr = err },
	}.Start(ctx, e)
	return errors.Join(err, shutdownErr)
}
