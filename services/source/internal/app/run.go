// Package app assembles the source process.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/config"
	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/scooter"
	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/tariffzone"
	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/userprofile"
)

// Run serves HTTP until the parent context is cancelled.
func Run(ctx context.Context) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	router := newRouter()

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

func newRouter() *echo.Echo {
	router := echo.New()
	router.Logger = slog.Default()
	router.GET("/healthz", func(c *echo.Context) error {
		return c.NoContent(http.StatusNoContent)
	})
	router.Any("/metrics", echo.WrapHandler(promhttp.Handler()))
	scooter.RegisterRoutes(router)
	tariffzone.RegisterRoutes(router)
	userprofile.RegisterRoutes(router)
	config.RegisterRoutes(router)
	return router
}
