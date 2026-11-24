package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

const maxBodyBytes = 64 << 10

var ErrInvalidJSON = errors.New("invalid JSON body")

// DecodeJSON reads a JSON body regardless of Content-Type, so plain `curl -d` and load
// generators that omit the header are accepted.
func DecodeJSON(c *echo.Context, dst any) error {
	body := http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes)
	if err := json.NewDecoder(body).Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	return nil
}
