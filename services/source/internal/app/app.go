// Package app serves the Source stub: scooters, tariff zones, user profiles and configs.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dkoptev06/sd-hw1-team4/services/source/internal/catalog"
)

// Run serves HTTP until ctx is done.
func Run(ctx context.Context) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	scooters, err := envInt("SOURCE_SCOOTERS", 40_000)
	if err != nil {
		return err
	}
	users, err := envInt("SOURCE_USERS", 40_000)
	if err != nil {
		return err
	}
	addr := os.Getenv("SOURCE_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	slog.Info("serving Source stub", "addr", addr, "scooters", scooters, "users", users)
	return start(ctx, addr, NewRouter(catalog.Seed(scooters, users)))
}

// NewRouter exposes the catalog through the agreed Source API.
func NewRouter(c *catalog.Catalog) *echo.Echo {
	e := echo.New()
	e.Logger = slog.Default()
	e.HTTPErrorHandler = func(ctx *echo.Context, err error) {
		status := echo.StatusCode(err)
		if status == 0 {
			status = http.StatusInternalServerError
		}
		_ = ctx.JSON(status, errorBody{ErrorCode: strings.ToUpper(strings.ReplaceAll(http.StatusText(status), " ", "_"))})
	}
	e.GET("/scooter/:id", byID(c.Scooter))
	e.GET("/tariff-zone/:id", byID(c.TariffZone))
	e.GET("/user-profile/:id", byID(c.UserProfile))
	e.GET("/configs", func(ctx *echo.Context) error {
		return ctx.JSON(http.StatusOK, c.Configs())
	})
	e.GET("/healthz", func(ctx *echo.Context) error { return ctx.NoContent(http.StatusNoContent) })
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	return e
}

type errorBody struct {
	ErrorCode string `json:"error_code"`
}

func byID[T any](lookup func(string) (T, bool)) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		id := ctx.Param("id")
		if id == "" {
			return ctx.JSON(http.StatusBadRequest, errorBody{ErrorCode: "INVALID_REQUEST"})
		}
		value, ok := lookup(id)
		if !ok {
			return ctx.JSON(http.StatusNotFound, errorBody{ErrorCode: "NOT_FOUND"})
		}
		return ctx.JSON(http.StatusOK, value)
	}
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

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: invalid non-negative integer %q", key, v)
	}
	return n, nil
}
