//go:build functional

package tests

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Stand struct {
	binary      string
	databaseURL string
	DB          *pgxpool.Pool
}

func NewStand(t *testing.T, binary string) *Stand {
	t.Helper()
	dsn := os.Getenv("RENTAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("set RENTAL_TEST_DATABASE_URL to run functional tests")
	}
	db := NewDB(t)
	schema := db.Config().ConnConfig.RuntimeParams["search_path"]

	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("invalid PostgreSQL URL")
		}
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}

	return &Stand{binary: binary, databaseURL: dsn, DB: db}
}

func (s *Stand) RentalCLI(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), s.binary, args...)
	cmd.Env = append(os.Environ(), "DATABASE_URL="+s.databaseURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rental %v: %v\n%s", args, err, output)
	}
	return string(output)
}

type ServiceProcess struct {
	URL  string
	stop func()
}

func (p *ServiceProcess) Stop() {
	p.stop()
}

func (s *Stand) StartRental(t *testing.T, role string, env ...string) *ServiceProcess {
	t.Helper()
	return startService(t, s.binary, "RENTAL_HTTP_ADDR", "/readyz", []string{role},
		append([]string{"DATABASE_URL=" + s.databaseURL}, env...)...)
}

func startService(t *testing.T, binary, addrEnv, probe string, args []string, env ...string) *ServiceProcess {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	// Own shutdown explicitly: t.Context() is canceled before test cleanups run.
	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(), addrEnv+"="+addr)
	cmd.Env = append(cmd.Env, env...)
	cmd.Env = append(cmd.Env, "GORACE="+os.Getenv("GORACE")+" log_path=stderr halt_on_error=1")
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s %v: %v", binary, args, err)
	}
	exited := make(chan struct{})
	var exitErr error
	go func() {
		exitErr = cmd.Wait()
		close(exited)
	}()
	stopProcess := sync.OnceFunc(func() {
		killErr := cmd.Process.Kill()
		<-exited
		// A deliberate crash (including cleanup) ends with SIGKILL on the test stand.
		var exit *exec.ExitError
		if killErr != nil || !errors.As(exitErr, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Errorf("%s %v exited unexpectedly: wait=%v, kill=%v", binary, args, exitErr, killErr)
		}
		if bytes.Contains(logs.Bytes(), []byte("WARNING: DATA RACE\n")) {
			t.Errorf("race detector reported a data race in %s %v", binary, args)
		}
	})
	t.Cleanup(func() {
		stopProcess()
		if t.Failed() {
			t.Logf("%s %v logs:\n%s", binary, args, logs.String())
		}
	})

	apiURL := "http://" + addr
	ready, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ready, http.MethodGet, apiURL+probe, nil)
	if err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return &ServiceProcess{URL: apiURL, stop: stopProcess}
			}
		}
		select {
		case <-exited:
			t.Fatalf("%s %v exited before readiness: %v", binary, args, exitErr)
		case <-ready.Done():
			t.Fatalf("%s %v did not become ready", binary, args)
		case <-ticker.C:
		}
	}
}
