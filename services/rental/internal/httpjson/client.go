// Package httpjson holds the HTTP client plumbing shared by the Source and Payment clients.
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// NewClient returns a client tuned for many concurrent calls to a single host.
// Per-call deadlines come from the request context.
func NewClient() *http.Client {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{Transport: transport}
}

// Response contains the HTTP status and the body bytes that were read.
type Response struct {
	Status int
	Body   []byte
}

// Do sends a request with an optional JSON body and reads the whole response.
// If reading the body fails, Do returns the HTTP status and the read error.
func Do(ctx context.Context, client *http.Client, method, url string, body any) (Response, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return Response{}, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return Response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return Response{Status: resp.StatusCode, Body: data}, err
}
