//go:build functional

package tests

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func MetricValue(client *http.Client, endpoint, metric string) (float64, error) {
	resp, err := client.Get(endpoint + "/metrics")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET /metrics: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	for line := range strings.SplitSeq(string(body), "\n") {
		if value, ok := strings.CutPrefix(line, metric+" "); ok {
			return strconv.ParseFloat(value, 64)
		}
	}
	return 0, nil
}

func WaitForMetric(t *testing.T, client *http.Client, endpoint, sample string) {
	t.Helper()
	WaitFor(t, sample, func() (bool, error) {
		resp, err := client.Get(endpoint + "/metrics")
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, fmt.Errorf("GET /metrics: HTTP %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		return strings.Contains(string(body), sample+"\n"), err
	})
	t.Logf("Метрика %s", sample)
}
