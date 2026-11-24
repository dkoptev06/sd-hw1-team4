//go:build functional

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

func RequestJSON[T any](client *http.Client, method, endpoint string, payload any) (int, T, error) {
	var body T
	var reader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, body, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		return 0, body, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, body, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body, err
}

func RequirePost[T any](t *testing.T, client *http.Client, endpoint string, payload any, wantStatus int) T {
	t.Helper()
	status, body, err := RequestJSON[T](client, http.MethodPost, endpoint, payload)
	if err != nil || status != wantStatus {
		t.Fatalf("POST %s: got HTTP %d, want %d. Response: %+v, error: %v", endpoint, status, wantStatus, body, err)
	}
	return body
}

func WaitFor(t *testing.T, condition string, check func() (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			t.Fatalf("%s: %v", condition, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", condition)
		case <-ticker.C:
		}
	}
}

func WriteJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}
