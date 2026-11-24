package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func newOpsRouter(log *slog.Logger, ready func(context.Context) error) *echo.Echo {
	e := echo.New()
	e.Logger = log
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
	return e
}

// startHTTPServer serves e on addr until ctx is done, then shuts down gracefully.
func startHTTPServer(ctx context.Context, addr string, e *echo.Echo) error {
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

// runWithOpsServer runs a background process with /healthz, /readyz and /metrics.
func runWithOpsServer(ctx context.Context, addr string, pool *pgxpool.Pool, log *slog.Logger, run func(context.Context) error) error {
	e := newOpsRouter(log, pool.Ping)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- startHTTPServer(ctx, addr, e) }()
	runErr := run(ctx)
	cancel()
	return errors.Join(runErr, <-serverErr)
}
