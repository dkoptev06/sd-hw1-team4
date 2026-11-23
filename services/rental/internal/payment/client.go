// Package payment is the Rental client of the Payment API.
package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpjson"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
)

// Outcome classifies a Payment command result for the worker.
type Outcome int

const (
	// Success is a final "success" answer.
	Success Outcome = iota
	// Rejected is a final "failed" answer (no hold, or a debt on settle).
	Rejected
	// ContractError is an unexpected 4xx: retrying the same call cannot help.
	ContractError
	// Transient is a 5xx, timeout or unreadable answer: the result is unknown and the call may be retried.
	Transient
)

func (o Outcome) String() string {
	switch o {
	case Success:
		return "success"
	case Rejected:
		return "rejected"
	case ContractError:
		return "contract_error"
	default:
		return "transient"
	}
}

// Result is the outcome of a command with a technical detail for last_error.
type Result struct {
	Outcome Outcome
	Detail  string
}

type Client struct {
	baseURL string
	http    *http.Client
	timeout time.Duration
}

func NewClient(baseURL string, client *http.Client, timeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: client, timeout: timeout}
}

func (c *Client) Hold(ctx context.Context, p domain.HoldParams) Result {
	return c.command(ctx, "hold", p)
}

func (c *Client) Settle(ctx context.Context, p domain.SettleParams) Result {
	return c.command(ctx, "settle", p)
}

func (c *Client) Void(ctx context.Context, p domain.VoidParams) Result {
	return c.command(ctx, "void", p)
}

func (c *Client) command(ctx context.Context, operation string, body any) Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	started := time.Now()
	resp, err := httpjson.Do(ctx, c.http, http.MethodPost, c.baseURL+"/payments/"+operation, body)
	metrics.DependencyDuration.WithLabelValues("payment", operation).Observe(time.Since(started).Seconds())
	result := classify(resp, err)
	metrics.DependencyCalls.WithLabelValues("payment", operation, result.Outcome.String()).Inc()
	return result
}

func classify(resp httpjson.Response, err error) Result {
	switch {
	case err != nil:
		if errors.Is(err, context.DeadlineExceeded) {
			return Result{Outcome: Transient, Detail: "timeout"}
		}
		return Result{Outcome: Transient, Detail: err.Error()}
	case resp.Status >= 400 && resp.Status < 500:
		return Result{Outcome: ContractError, Detail: fmt.Sprintf("status %d: %s", resp.Status, truncate(resp.Body))}
	case resp.Status != http.StatusOK:
		return Result{Outcome: Transient, Detail: fmt.Sprintf("status %d", resp.Status)}
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return Result{Outcome: Transient, Detail: "unreadable body: " + err.Error()}
	}
	switch body.Status {
	case domain.ResultSuccess:
		return Result{Outcome: Success}
	case domain.ResultFailed:
		return Result{Outcome: Rejected}
	default:
		return Result{Outcome: Transient, Detail: fmt.Sprintf("unknown status %q", body.Status)}
	}
}

func truncate(body []byte) string {
	const limit = 200
	if len(body) > limit {
		return string(body[:limit])
	}
	return string(body)
}
