package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLoadtestScenarioChild(t *testing.T) {
	if os.Getenv("LOADTEST_SCENARIO_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator == -1 {
		t.Fatal("missing command arguments")
	}
	os.Args = append([]string{"loadtest"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet("loadtest", flag.ExitOnError)
	main()
	os.Exit(0) // only emit the loadtest JSON, without the test runner's PASS
}

type scenarioRide struct {
	scooter string
	started time.Time
	ended   *time.Time
	reads   int
}

func TestLoadtestScenarioContract(t *testing.T) {
	for _, tc := range []struct {
		name         string
		history      int
		breakHistory bool
	}{
		{name: "baseline"},
		{name: "mixed", history: 15},
		{name: "bad_archive_response_fails", history: 15, breakHistory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const scheduled = 30 // 5 arrivals/s across 1s of warm-up and a 5s window
			const fleet = 7
			var mu sync.Mutex
			offers := make(map[string]*scenarioRide)
			orders := make(map[string]*scenarioRide)
			occupied := make(map[string]bool)
			idempotency := make(map[string]bool)
			secondArrival := make(chan struct{})
			var archiveReads int
			archive := map[string]bool{
				"eeead460-8e70-5d56-af1b-a441a271c024": true,
				"912ba484-caf1-5a00-9b0d-446400cdfec7": true,
			}
			historyFile := filepath.Join(t.TempDir(), "history.ids")
			if err := os.WriteFile(historyFile, []byte("eeead460-8e70-5d56-af1b-a441a271c024\n912ba484-caf1-5a00-9b0d-446400cdfec7\n"), 0600); err != nil {
				t.Fatal(err)
			}

			mux := http.NewServeMux()
			respond := func(w http.ResponseWriter, status int, body any) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if err := json.NewEncoder(w).Encode(body); err != nil {
					t.Errorf("write response: %v", err)
				}
			}
			mux.HandleFunc("POST /offers", func(w http.ResponseWriter, req *http.Request) {
				var body map[string]string
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				mu.Lock()
				scooter, user, key := body["scooter_id"], body["user_id"], body["idempotency_id"]
				index, ok := strings.CutPrefix(scooter, "scooter-")
				pair, err := strconv.Atoi(index)
				if !ok || err != nil || pair < 1 || pair > fleet || user != "user-"+index || occupied[scooter] || key == "" || idempotency[key] {
					mu.Unlock()
					t.Errorf("invalid fleet or idempotency assignment for %q", scooter)
					w.WriteHeader(409)
					return
				}
				idempotency[key] = true
				occupied[scooter] = true
				id := uuid.NewString()
				offers[id] = &scenarioRide{scooter: scooter}
				ordinal := len(offers)
				mu.Unlock()
				switch ordinal {
				case 1:
					// The first request only completes after arrival of the second.
					// A closed-loop driver would never send the second in time.
					select {
					case <-secondArrival:
					case <-time.After(1500 * time.Millisecond):
						t.Error("the next arrival waited for the previous response")
						w.WriteHeader(504)
						return
					}
				case 2:
					close(secondArrival)
				}
				respond(w, 201, map[string]any{"offer_id": id, "minute_rate": 60, "unlock_fee": 10, "deposit": 200})
			})
			mux.HandleFunc("POST /orders", func(w http.ResponseWriter, req *http.Request) {
				var body map[string]string
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				id := body["offer_id"]
				ride, ok := offers[id]
				if !ok || !ride.started.IsZero() {
					t.Errorf("start failed to use a fresh offer %q", id)
					w.WriteHeader(409)
					return
				}
				ride.started = time.Now().Add(-time.Minute)
				orders[id] = ride
				respond(w, 202, map[string]any{"order_id": id, "status": "STARTING"})
			})
			mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				id := req.PathValue("id")
				if archive[id] {
					archiveReads++
					if tc.breakHistory {
						respond(w, 503, map[string]string{"error_code": "FIXTURE_UNAVAILABLE"})
						return
					}
					end := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
					respond(w, 200, map[string]any{"order_id": id, "status": "COMPLETED", "cost": 130,
						"started_at": end.Add(-2 * time.Minute), "finish_time": end})
					return
				}
				ride, ok := orders[id]
				if !ok {
					t.Errorf("GET used unknown ID %q", id)
					w.WriteHeader(404)
					return
				}
				ride.reads++
				status, at := "ACTIVE", time.Now()
				if ride.ended != nil {
					status, at = "COMPLETED", *ride.ended
				}
				respond(w, 200, map[string]any{"order_id": id, "status": status,
					"cost": 10 + int64(at.Sub(ride.started)/time.Second), "started_at": ride.started, "finish_time": ride.ended})
			})
			mux.HandleFunc("POST /orders/{id}/finish", func(w http.ResponseWriter, req *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				id := req.PathValue("id")
				ride, ok := orders[id]
				if !ok || ride.ended != nil {
					t.Errorf("finish used a missing or already closed ride %q", id)
					w.WriteHeader(409)
					return
				}
				at := time.Now()
				ride.ended = &at
				delete(occupied, ride.scooter)
				respond(w, 200, map[string]any{"order_id": id, "status": "COMPLETED",
					"cost": 10 + int64(at.Sub(ride.started)/time.Second), "finish_time": at})
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"-test.run=^TestLoadtestScenarioChild$", "--", "-url", server.URL,
				"-rate", "5", "-window", "5s", "-warmup", "1s", "-min-ride", "250ms", "-max-ride", "250ms",
				"-fleet", strconv.Itoa(fleet), "-read-concurrency", "16", "-read-queue", "2048"}
			if tc.history > 0 {
				args = append(args, "-historical", "15", "-history-file", historyFile)
			}
			cmd := exec.CommandContext(t.Context(), binary, args...)
			cmd.Env = append(os.Environ(), "LOADTEST_SCENARIO_CHILD=1")
			raw, err := cmd.Output()
			if tc.breakHistory {
				if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 1 {
					t.Fatalf("expected exit 1 for failed history: %v, %s", err, raw)
				}
			} else if err != nil {
				if exit, ok := err.(*exec.ExitError); ok {
					t.Fatalf("%v\nstderr: %s\nreport: %s", err, exit.Stderr, raw)
				}
				t.Fatal(err)
			}

			var report struct {
				Pass      bool                 `json:"http_profile_pass"`
				Scheduled int                  `json:"scheduled_trips"`
				Accepted  int                  `json:"accepted_trips"`
				Finished  int                  `json:"completed_trips"`
				Reads     map[string]getCounts `json:"primary_gets"`
				Failures  map[string]int       `json:"all_failures"`
			}
			if err := json.Unmarshal(raw, &report); err != nil {
				t.Fatalf("read report: %v, %s", err, raw)
			}
			if report.Pass == tc.breakHistory || report.Scheduled != scheduled || report.Accepted != scheduled || report.Finished != scheduled {
				t.Fatalf("unexpected report: %+v", report)
			}
			current := scheduled * (50 - tc.history)
			history := scheduled * tc.history
			wantHistory := history
			wantFailures := 0
			if tc.breakHistory {
				wantHistory = 0
				wantFailures = 1
			}
			if report.Reads["get_current"] != (getCounts{current, current, current}) ||
				report.Reads["get_history"] != (getCounts{history, history, wantHistory}) ||
				report.Failures["get_history/503/FIXTURE_UNAVAILABLE"] != history-wantHistory ||
				len(report.Failures) != wantFailures {
				t.Fatalf("unexpected read or error counts: %+v", report)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(orders) != scheduled || len(occupied) != 0 || archiveReads != history {
				t.Fatalf("fixture observed orders=%d occupied=%d archiveReads=%d", len(orders), len(occupied), archiveReads)
			}
			for id, ride := range orders {
				if ride.ended == nil || ride.reads != 50-tc.history {
					t.Errorf("ride %q closed=%t, current GETs=%d", id, ride.ended != nil, ride.reads)
				}
			}
		})
	}
}
