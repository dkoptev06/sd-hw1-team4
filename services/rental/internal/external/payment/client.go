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

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/metrics"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

type Client struct {
	baseURL string
	http    *http.Client
	timeout time.Duration
}

func NewClient(baseURL string, client *http.Client, timeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: client, timeout: timeout}
}

func (c *Client) Hold(ctx context.Context, p task.HoldParams) task.Result {
	return c.command(ctx, "hold", holdRequest(p))
}

func (c *Client) Settle(ctx context.Context, p task.SettleParams) task.Result {
	return c.command(ctx, "settle", settleRequest(p))
}

func (c *Client) Void(ctx context.Context, p task.VoidParams) task.Result {
	return c.command(ctx, "void", voidRequest(p))
}

func (c *Client) command(ctx context.Context, operation string, body any) task.Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	started := time.Now()
	resp, err := httpx.Do(ctx, c.http, http.MethodPost, c.baseURL+"/payments/"+operation, body)
	metrics.DependencyDuration.WithLabelValues("payment", operation).Observe(time.Since(started).Seconds())
	result := classify(resp, err)
	metrics.DependencyCalls.WithLabelValues("payment", operation, result.Outcome.String()).Inc()
	return result
}

func classify(resp httpx.Response, err error) task.Result {
	switch {
	case resp.Status >= 400 && resp.Status < 500:
		detail := errorBodySnippet(resp.Body)
		if err != nil {
			detail = "read body: " + err.Error()
		}
		return task.Result{Outcome: task.ContractError, Detail: fmt.Sprintf("status %d: %s", resp.Status, detail)}
	case err != nil:
		if errors.Is(err, context.DeadlineExceeded) {
			return task.Result{Outcome: task.Transient, Detail: "timeout"}
		}
		return task.Result{Outcome: task.Transient, Detail: err.Error()}
	case resp.Status != http.StatusOK:
		return task.Result{Outcome: task.Transient, Detail: fmt.Sprintf("status %d", resp.Status)}
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return task.Result{Outcome: task.Transient, Detail: "unreadable body: " + err.Error()}
	}
	switch body.Status {
	case task.ResultSuccess:
		return task.Result{Outcome: task.Success}
	case task.ResultFailed:
		return task.Result{Outcome: task.Rejected}
	default:
		return task.Result{Outcome: task.Transient, Detail: fmt.Sprintf("unknown status %q", body.Status)}
	}
}

// PostgreSQL text requires valid UTF-8 and excludes NUL. The byte limit can split a rune.
func errorBodySnippet(body []byte) string {
	const limit = 200
	snippet := strings.ToValidUTF8(string(body[:min(len(body), limit)]), "?")
	return strings.ReplaceAll(snippet, "\x00", "?")
}
