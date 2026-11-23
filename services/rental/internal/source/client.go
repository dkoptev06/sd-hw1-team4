// Package source is the Rental client of the Source API (scooters, tariff zones,
// user profiles and configs) and its tariff zone cache.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpjson"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
)

var (
	// ErrNotFound means Source answered 404 for the requested ID.
	ErrNotFound = errors.New("source: not found")
	// ErrUnavailable means Source failed, timed out or broke its contract.
	ErrUnavailable = errors.New("source: unavailable")
)

// Configs is the raw /configs response; dynconfig validates it.
type Configs struct {
	PriceCoeffSettings struct {
		Surge             json.Number `json:"surge"`
		LowChargeDiscount json.Number `json:"low_charge_discount"`
	} `json:"price_coeff_settings"`
	DefaultRetentionDays      int            `json:"default_retention_days"`
	RetentionDaysByZone       map[string]int `json:"retention_days_by_zone"`
	TariffZoneCacheTTLMinutes *int           `json:"tariff_zone_cache_ttl_minutes"`
}

type Client struct {
	baseURL       string
	http          *http.Client
	timeout       time.Duration
	configTimeout time.Duration
}

func NewClient(baseURL string, client *http.Client, timeout, configTimeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: client, timeout: timeout, configTimeout: configTimeout}
}

func (c *Client) Scooter(ctx context.Context, id string) (domain.Scooter, error) {
	var body struct {
		ID     string `json:"id"`
		ZoneID string `json:"zone_id"`
		Charge *int   `json:"charge"`
	}
	if err := c.get(ctx, "scooter", "/scooter/"+url.PathEscape(id), c.timeout, &body); err != nil {
		return domain.Scooter{}, err
	}
	if body.Charge == nil {
		return domain.Scooter{}, fmt.Errorf("%w: scooter without charge", ErrUnavailable)
	}
	scooter := domain.Scooter{ID: body.ID, ZoneID: body.ZoneID, Charge: *body.Charge}
	if err := scooter.Validate(); err != nil || scooter.ID != id {
		return domain.Scooter{}, fmt.Errorf("%w: invalid scooter %q: %v", ErrUnavailable, id, err)
	}
	return scooter, nil
}

func (c *Client) TariffZone(ctx context.Context, id string) (domain.Tariff, error) {
	var body struct {
		ID             string `json:"id"`
		PricePerMinute *int64 `json:"price_per_minute"`
		PriceUnlock    *int64 `json:"price_unlock"`
		DefaultDeposit *int64 `json:"default_deposit"`
	}
	if err := c.get(ctx, "tariff_zone", "/tariff-zone/"+url.PathEscape(id), c.timeout, &body); err != nil {
		return domain.Tariff{}, err
	}
	if body.PricePerMinute == nil || body.PriceUnlock == nil || body.DefaultDeposit == nil {
		return domain.Tariff{}, fmt.Errorf("%w: tariff zone %q has missing prices", ErrUnavailable, id)
	}
	tariff := domain.Tariff{
		ZoneID: body.ID, PricePerMinute: *body.PricePerMinute,
		PriceUnlock: *body.PriceUnlock, DefaultDeposit: *body.DefaultDeposit,
	}
	if err := tariff.Validate(); err != nil || tariff.ZoneID != id {
		return domain.Tariff{}, fmt.Errorf("%w: invalid tariff zone %q: %v", ErrUnavailable, id, err)
	}
	return tariff, nil
}

func (c *Client) UserProfile(ctx context.Context, id string) (domain.UserProfile, error) {
	var body struct {
		ID              string `json:"id"`
		HasSubscription *bool  `json:"has_subscription"`
		Trusted         *bool  `json:"trusted"`
	}
	if err := c.get(ctx, "user_profile", "/user-profile/"+url.PathEscape(id), c.timeout, &body); err != nil {
		return domain.UserProfile{}, err
	}
	if body.ID != id || body.HasSubscription == nil || body.Trusted == nil {
		return domain.UserProfile{}, fmt.Errorf("%w: invalid user profile %q", ErrUnavailable, id)
	}
	return domain.UserProfile{UserID: id, HasSubscription: *body.HasSubscription, Trusted: *body.Trusted}, nil
}

func (c *Client) Configs(ctx context.Context) (Configs, error) {
	var body Configs
	err := c.get(ctx, "configs", "/configs", c.configTimeout, &body)
	return body, err
}

func (c *Client) get(ctx context.Context, operation, path string, timeout time.Duration, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	resp, err := httpjson.Do(ctx, c.http, http.MethodGet, c.baseURL+path, nil)
	metrics.DependencyDuration.WithLabelValues("source", operation).Observe(time.Since(started).Seconds())

	outcome, result := classify(resp, err, out)
	metrics.DependencyCalls.WithLabelValues("source", operation, outcome).Inc()
	if result != nil {
		return fmt.Errorf("GET %s: %w", path, result)
	}
	return nil
}

func classify(resp httpjson.Response, err error, out any) (string, error) {
	switch {
	case err != nil:
		if errors.Is(err, context.DeadlineExceeded) {
			return "timeout", fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		return "network_error", fmt.Errorf("%w: %v", ErrUnavailable, err)
	case resp.Status == http.StatusNotFound:
		return "not_found", ErrNotFound
	case resp.Status != http.StatusOK:
		return fmt.Sprintf("http_%d", resp.Status), fmt.Errorf("%w: status %d", ErrUnavailable, resp.Status)
	}
	if err := json.Unmarshal(resp.Body, out); err != nil {
		return "invalid_body", fmt.Errorf("%w: decode body: %v", ErrUnavailable, err)
	}
	return "ok", nil
}
