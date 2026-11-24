// Package httpapi exposes the Rental client API over HTTP/JSON.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpserver"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
)

// NewRouter builds the API with health and metrics endpoints. Handlers return
// domain errors; errorWriter turns them into {"error_code": ...} responses.
func NewRouter(api *API, ready func(context.Context) error, log *slog.Logger) *echo.Echo {
	e := httpserver.New(log)
	errs := errorWriter{log: log}
	e.HTTPErrorHandler = func(c *echo.Context, err error) { errs.write(c, err) }
	httpserver.RegisterOps(e, ready)

	observe := errs.instrument
	e.POST("/offers", api.createOffer, observe)
	e.POST("/orders", api.startOrder, observe)
	e.GET("/orders/:order_id", api.getOrder, observe)
	e.POST("/orders/:order_id/finish", api.finishOrder, observe)
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

var statusByCode = map[domain.Code]int{
	domain.CodeInvalidRequest:    http.StatusBadRequest,
	domain.CodeNotFound:          http.StatusNotFound,
	domain.CodeParameterConflict: http.StatusConflict,
	domain.CodeOfferExpired:      http.StatusConflict,
	domain.CodeOfferChanged:      http.StatusConflict,
	domain.CodeScooterBusy:       http.StatusConflict,
	domain.CodeOrderNotActive:    http.StatusConflict,
	domain.CodeSourceUnavailable: http.StatusServiceUnavailable,
	domain.CodeZoneStale:         http.StatusServiceUnavailable,
	domain.CodeOverloaded:        http.StatusServiceUnavailable,
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
	if businessErr, ok := errors.AsType[*domain.Error](err); ok {
		if status, ok := statusByCode[businessErr.Code]; ok {
			return status, string(businessErr.Code)
		}
	}
	// Echo routing errors: unknown route or method.
	switch echo.StatusCode(err) {
	case http.StatusNotFound:
		return http.StatusNotFound, string(domain.CodeNotFound)
	case http.StatusMethodNotAllowed:
		return http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED"
	}
	if c.Request().Context().Err() == nil {
		w.log.Error("request failed", "method", c.Request().Method, "path", c.Request().URL.Path, "error", err)
	}
	return http.StatusInternalServerError, codeInternal
}
