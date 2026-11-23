// Package settings loads Rental process settings from the environment.
package settings

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/domain"
)

type Settings struct {
	HTTPAddr    string
	DatabaseURL string
	DBMaxConns  int32

	SourceURL       string
	SourceTimeout   time.Duration
	ConfigTimeout   time.Duration
	ConfigRefresh   time.Duration
	TariffCacheSize int

	PaymentURL     string
	PaymentTimeout time.Duration

	OfferTTL  time.Duration
	StartTTL  time.Duration
	HoldGrace time.Duration

	Worker      Worker
	Maintenance Maintenance
}

type Worker struct {
	Concurrency     int
	BatchSize       int
	PollInterval    time.Duration
	SweepInterval   time.Duration
	Lease           time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	ShutdownTimeout time.Duration
	MaxAttempts     map[domain.TaskKind]int
}

type Maintenance struct {
	Interval      time.Duration
	Lease         time.Duration
	BatchSize     int
	OfferGrace    time.Duration
	WeeksAhead    int
	StatsInterval time.Duration
}

// Load reads settings; every value except DATABASE_URL has a default.
func Load() (Settings, error) {
	e := env{}
	s := Settings{
		HTTPAddr:    e.str("RENTAL_HTTP_ADDR", ":8080"),
		DatabaseURL: e.str("DATABASE_URL", ""),
		DBMaxConns:  int32(e.int("RENTAL_DB_MAX_CONNS", 20)),

		SourceURL:       e.str("SOURCE_URL", "http://source:8080"),
		SourceTimeout:   e.dur("SOURCE_TIMEOUT", 500*time.Millisecond),
		ConfigTimeout:   e.dur("CONFIG_TIMEOUT", 2*time.Second),
		ConfigRefresh:   e.dur("CONFIG_REFRESH_INTERVAL", time.Minute),
		TariffCacheSize: e.int("TARIFF_CACHE_SIZE", 10_000),

		PaymentURL:     e.str("PAYMENT_URL", "http://payment:8080"),
		PaymentTimeout: e.dur("PAYMENT_TIMEOUT", 3*time.Second),

		OfferTTL:  e.dur("OFFER_TTL", 2*time.Minute),
		StartTTL:  e.dur("START_TTL", 2*time.Minute),
		HoldGrace: e.dur("HOLD_GRACE", time.Hour),

		Worker: Worker{
			Concurrency:     e.int("WORKER_CONCURRENCY", 64),
			BatchSize:       e.int("WORKER_BATCH_SIZE", 32),
			PollInterval:    e.dur("WORKER_POLL_INTERVAL", 100*time.Millisecond),
			SweepInterval:   e.dur("WORKER_SWEEP_INTERVAL", time.Second),
			Lease:           e.dur("WORKER_LEASE", 15*time.Second),
			BackoffBase:     e.dur("WORKER_BACKOFF_BASE", 500*time.Millisecond),
			BackoffMax:      e.dur("WORKER_BACKOFF_MAX", 5*time.Minute),
			ShutdownTimeout: e.dur("WORKER_SHUTDOWN_TIMEOUT", 20*time.Second),
			MaxAttempts: map[domain.TaskKind]int{
				domain.TaskStart:  e.int("START_MAX_ATTEMPTS", 5),
				domain.TaskSettle: e.int("SETTLE_MAX_ATTEMPTS", 20),
				domain.TaskVoid:   e.int("VOID_MAX_ATTEMPTS", 20),
			},
		},
		Maintenance: Maintenance{
			Interval:      e.dur("MAINTENANCE_INTERVAL", time.Minute),
			Lease:         e.dur("MAINTENANCE_LEASE", 5*time.Minute),
			BatchSize:     e.int("MAINTENANCE_BATCH_SIZE", 2000),
			OfferGrace:    e.dur("MAINTENANCE_OFFER_GRACE", 10*time.Minute),
			WeeksAhead:    e.int("MAINTENANCE_PARTITION_WEEKS_AHEAD", 54),
			StatsInterval: e.dur("MAINTENANCE_STATS_INTERVAL", 15*time.Second),
		},
	}
	if s.DatabaseURL == "" {
		e.errs = append(e.errs, errors.New("DATABASE_URL is required"))
	}
	for name, v := range map[string]int{
		"RENTAL_DB_MAX_CONNS": int(s.DBMaxConns), "TARIFF_CACHE_SIZE": s.TariffCacheSize,
		"WORKER_CONCURRENCY": s.Worker.Concurrency, "WORKER_BATCH_SIZE": s.Worker.BatchSize,
		"MAINTENANCE_BATCH_SIZE": s.Maintenance.BatchSize, "MAINTENANCE_PARTITION_WEEKS_AHEAD": s.Maintenance.WeeksAhead,
	} {
		if v <= 0 {
			e.errs = append(e.errs, fmt.Errorf("%s must be positive", name))
		}
	}
	for kind, v := range s.Worker.MaxAttempts {
		if v <= 0 {
			e.errs = append(e.errs, fmt.Errorf("max attempts for %s must be positive", kind))
		}
	}
	if s.PaymentTimeout >= s.Worker.Lease {
		e.errs = append(e.errs, errors.New("PAYMENT_TIMEOUT must be shorter than WORKER_LEASE"))
	}
	return s, errors.Join(e.errs...)
}

type env struct{ errs []error }

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s: invalid positive duration %q", key, v))
	}
	return d
}
