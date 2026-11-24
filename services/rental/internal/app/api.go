package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/redis"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/offer"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func runAPI(ctx context.Context, cfg settings, pool *pgxpool.Pool, log *slog.Logger) error {
	registerPoolMetrics(pool)
	sourceClient := source.NewClient(cfg.SourceURL, httpx.NewClient(), cfg.SourceTimeout, cfg.ConfigTimeout)
	configs := dynconfig.NewProvider(sourceClient, cfg.ConfigRefresh, log)
	configs.Start(ctx)
	tariffs, err := source.NewTariffCache(cfg.TariffCacheSize, sourceClient.TariffZone, configs.TariffTTL, time.Now)
	if err != nil {
		return err
	}

	offers := offer.NewService(pool, sourceClient, tariffs, sourceClient, configs, cfg.OfferTTL)
	tasks := task.NewService(pool, cfg.Tasks)
	orders := order.NewService(pool, offers, tasks, sourceClient, configs, cfg.StartTTL, cfg.HoldGrace, log)
	// Reserve pool capacity for writes and readiness checks.
	cache := cfg.OrderCache
	cache.DBMaxConcurrency = min(cache.DBMaxConcurrency, max(1, int(cfg.DBMaxConns)-1))
	cache.DBDegradedConcurrency = min(cache.DBDegradedConcurrency, cache.DBMaxConcurrency)
	var remote order.RemoteCache
	if cache.RedisURL != "" {
		remote, err = redis.NewClient(cache.RedisURL, cache.RedisPoolSize, cache.RedisTimeout)
		if err != nil {
			return err
		}
	}
	orders, err = orders.WithArchiveCache(cache.CacheOptions, remote)
	if err != nil {
		if remote != nil {
			_ = remote.Close()
		}
		return err
	}
	defer orders.Close()

	router := newAPIRouter(pool.Ping, log, offers.RegisterRoutes, orders.RegisterRoutes)
	log.Info("serving API", "addr", cfg.HTTPAddr)
	return startHTTPServer(ctx, cfg.HTTPAddr, router)
}

// newAPIRouter builds the API with health and metrics endpoints. Handlers return
// domain errors; errorWriter turns them into {"error_code": ...} responses.
func newAPIRouter(ready func(context.Context) error, log *slog.Logger,
	routes ...func(*echo.Echo, ...echo.MiddlewareFunc),
) *echo.Echo {
	e := newOpsRouter(log, ready)
	errs := errorWriter{log: log}
	e.HTTPErrorHandler = func(c *echo.Context, err error) { errs.write(c, err) }

	for _, register := range routes {
		register(e, errs.instrument)
	}
	return e
}

type errorWriter struct {
	log *slog.Logger
}

// instrument writes the error response itself, so it knows the final status and
// error code, and records API metrics by route.
func (w errorWriter) instrument(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		started := time.Now()
		code := ""
		if err := next(c); err != nil {
			code = w.write(c, err)
		}
		_, status := echo.ResolveResponseStatus(c.Response(), nil)
		route := c.Request().Method + " " + c.Path()
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(status), code).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(started).Seconds())
		return nil
	}
}

type errorBody struct {
	ErrorCode string `json:"error_code"`
}

const codeInternal = "INTERNAL_ERROR"

var apiErrors = []struct {
	sentinel error
	status   int
}{
	{offer.ErrInvalidRequest, http.StatusBadRequest},
	{offer.ErrNotFound, http.StatusNotFound},
	{offer.ErrParameterConflict, http.StatusConflict},
	{offer.ErrOfferExpired, http.StatusConflict},
	{offer.ErrOfferChanged, http.StatusConflict},
	{offer.ErrSourceUnavailable, http.StatusServiceUnavailable},
	{order.ErrInvalidRequest, http.StatusBadRequest},
	{order.ErrNotFound, http.StatusNotFound},
	{order.ErrScooterBusy, http.StatusConflict},
	{order.ErrOrderNotActive, http.StatusConflict},
	{order.ErrSourceUnavailable, http.StatusServiceUnavailable},
	{order.ErrOverloaded, http.StatusServiceUnavailable},
	{source.ErrZoneStale, http.StatusServiceUnavailable},
	{source.ErrTariffUnavailable, http.StatusServiceUnavailable},
}

// write sends the error response and returns its error code.
func (w errorWriter) write(c *echo.Context, err error) string {
	if resp, _ := echo.UnwrapResponse(c.Response()); resp != nil && resp.Committed {
		return ""
	}
	status, code := w.classify(c, err)
	if writeErr := c.JSON(status, errorBody{ErrorCode: code}); writeErr != nil {
		w.log.Warn("write error response", "error", writeErr)
	}
	return code
}

func (w errorWriter) classify(c *echo.Context, err error) (int, string) {
	if errors.Is(err, httpx.ErrInvalidJSON) {
		return http.StatusBadRequest, "INVALID_REQUEST"
	}
	for _, mapping := range apiErrors {
		if errors.Is(err, mapping.sentinel) {
			return mapping.status, mapping.sentinel.Error()
		}
	}
	// Echo routing errors: unknown route or method.
	switch echo.StatusCode(err) {
	case http.StatusNotFound:
		return http.StatusNotFound, "NOT_FOUND"
	case http.StatusMethodNotAllowed:
		return http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED"
	}
	if c.Request().Context().Err() == nil {
		w.log.Error("request failed", "method", c.Request().Method, "path", c.Request().URL.Path, "error", err)
	}
	return http.StatusInternalServerError, codeInternal
}
