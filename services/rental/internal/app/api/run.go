// Package api assembles the Rental HTTP process and its migration command.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/config"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/migrations"
)

// Run serves Rental or applies its own migrations when passed the migrate command.
func Run(ctx context.Context, args []string) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	settings, err := config.Load()
	if err != nil {
		return err
	}
	pool, err := postgres.OpenPool(ctx, settings.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if len(args) > 0 {
		if len(args) != 1 || args[0] != "migrate" {
			return fmt.Errorf("usage: rental [migrate]")
		}
		db := stdlib.OpenDBFromPool(pool)
		defer db.Close()
		return migrations.Up(ctx, db)
	}

	router := newRouter(pool.Ping)

	var shutdownErr error
	server := echo.StartConfig{
		Address:         ":8080",
		HideBanner:      true,
		GracefulTimeout: 5 * time.Second,
		BeforeServeFunc: func(server *http.Server) error {
			server.ReadHeaderTimeout = 5 * time.Second
			server.ReadTimeout = 0
			return nil
		},
		OnShutdownError: func(err error) { shutdownErr = err },
	}
	if err := server.Start(ctx, router); err != nil {
		return err
	}
	return shutdownErr
}

func newRouter(ping func(context.Context) error) *echo.Echo {
	router := echo.New()
	router.Logger = slog.Default()
	router.GET("/healthz", func(c *echo.Context) error {
		return c.NoContent(http.StatusNoContent)
	})
	router.GET("/readyz", func(c *echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), time.Second)
		defer cancel()
		if err := ping(ctx); err != nil {
			return c.NoContent(http.StatusServiceUnavailable)
		}
		return c.NoContent(http.StatusNoContent)
	})
	router.Any("/metrics", echo.WrapHandler(promhttp.Handler()))
	offer.RegisterRoutes(router)
	order.RegisterRoutes(router)
	return router
}
