// Package app assembles the Rental processes: api, worker, maintenance, migrate and dlq.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpapi"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpjson"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/httpserver"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/maintenance"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/ordercache"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/payment"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/rental"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/settings"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/worker"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/migrations"
)

const usage = `usage: rental <command>

commands:
  api                        serve the client API
  worker                     execute START, SETTLE and VOID tasks
  maintenance                archive orders and clean up data
  migrate                    apply database migrations
  dlq list                   list dead-lettered tasks
  dlq requeue <task_id>|--all  return dead-lettered tasks to PENDING`

// Run starts the process selected by args.
func Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("process", args[0])
	slog.SetDefault(log)

	cfg, err := settings.Load()
	if err != nil {
		return err
	}
	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer store.Close()

	switch args[0] {
	case "api":
		return runAPI(ctx, cfg, store, log)
	case "worker":
		return runWorker(ctx, cfg, store, log)
	case "maintenance":
		return runMaintenance(ctx, cfg, store, log)
	case "migrate":
		db := stdlib.OpenDBFromPool(store.Pool())
		defer db.Close()
		return migrations.Up(ctx, db)
	case "dlq":
		return runDLQ(ctx, store, args[1:])
	default:
		return errors.New(usage)
	}
}

func runAPI(ctx context.Context, cfg settings.Settings, store *storage.Store, log *slog.Logger) error {
	registerPoolMetrics(store.Pool())
	sourceClient := source.NewClient(cfg.SourceURL, httpjson.NewClient(), cfg.SourceTimeout, cfg.ConfigTimeout)
	configs := dynconfig.NewProvider(sourceClient, cfg.ConfigRefresh, log)
	configs.Start(ctx)
	tariffs, err := source.NewTariffCache(cfg.TariffCacheSize, sourceClient.TariffZone, configs.TariffTTL, time.Now)
	if err != nil {
		return err
	}

	// Reserve pool capacity for the write endpoints and readiness checks.
	cacheCfg := cfg.OrderCache
	cacheCfg.DBMaxConcurrency = min(cacheCfg.DBMaxConcurrency, max(1, int(cfg.DBMaxConns)-1))
	cacheCfg.DBDegradedConcurrency = min(cacheCfg.DBDegradedConcurrency, cacheCfg.DBMaxConcurrency)
	var remote ordercache.RemoteCache
	if cacheCfg.RedisURL != "" {
		remote, err = ordercache.NewRedisClient(cacheCfg.RedisURL, cacheCfg.RedisPoolSize, cacheCfg.RedisTimeout)
		if err != nil {
			return err
		}
	}
	cache, err := ordercache.New(cacheCfg, remote, store.GetOrder, time.Now)
	if err != nil {
		if remote != nil {
			_ = remote.Close()
		}
		return err
	}
	defer cache.Close()

	offers := rental.NewOfferService(store, sourceClient, tariffs, sourceClient, configs, cfg.OfferTTL)
	orders := rental.NewOrderService(store, sourceClient, configs, cfg.StartTTL, cfg.HoldGrace, cache.Get)
	router := httpapi.NewRouter(httpapi.NewAPI(offers, orders), store.Ping, log)
	log.Info("serving API", "addr", cfg.HTTPAddr)
	return httpserver.Start(ctx, cfg.HTTPAddr, router)
}

func runWorker(ctx context.Context, cfg settings.Settings, store *storage.Store, log *slog.Logger) error {
	registerPoolMetrics(store.Pool())
	sourceClient := source.NewClient(cfg.SourceURL, httpjson.NewClient(), cfg.SourceTimeout, cfg.ConfigTimeout)
	configs := dynconfig.NewProvider(sourceClient, cfg.ConfigRefresh, log)
	configs.Start(ctx)
	payments := payment.NewClient(cfg.PaymentURL, httpjson.NewClient(), cfg.PaymentTimeout)

	w := worker.New(store, payments, configs, cfg.Worker, log)
	log.Info("worker started", "concurrency", cfg.Worker.Concurrency)
	return runWithOpsServer(ctx, cfg.HTTPAddr, store, log, w.Run)
}

func runMaintenance(ctx context.Context, cfg settings.Settings, store *storage.Store, log *slog.Logger) error {
	registerPoolMetrics(store.Pool())
	m := maintenance.New(store, cfg.Maintenance, log)
	log.Info("maintenance started", "interval", cfg.Maintenance.Interval.String())
	return runWithOpsServer(ctx, cfg.HTTPAddr, store, log, m.Run)
}

// runWithOpsServer runs a background process with /healthz, /readyz and /metrics.
func runWithOpsServer(ctx context.Context, addr string, store *storage.Store, log *slog.Logger, run func(context.Context) error) error {
	e := httpserver.New(log)
	httpserver.RegisterOps(e, store.Ping)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- httpserver.Start(ctx, addr, e) }()
	runErr := run(ctx)
	cancel()
	return errors.Join(runErr, <-serverErr)
}

func registerPoolMetrics(pool *pgxpool.Pool) {
	gauges := map[string]func(*pgxpool.Stat) float64{
		"acquired": func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) },
		"idle":     func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) },
		"total":    func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) },
		"max":      func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) },
	}
	for state, value := range gauges {
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "rental", Name: "db_pool_connections", Help: "PostgreSQL pool connections by state.",
			ConstLabels: prometheus.Labels{"state": state},
		}, func() float64 { return value(pool.Stat()) }))
	}
	prometheus.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "rental", Name: "db_pool_acquire_wait_seconds_total", Help: "Time spent waiting for a pool connection.",
	}, func() float64 { return pool.Stat().AcquireDuration().Seconds() }))
}

func runDLQ(ctx context.Context, store *storage.Store, args []string) error {
	switch {
	case len(args) == 1 && args[0] == "list":
		tasks, err := store.ListDeadLetters(ctx)
		if err != nil {
			return err
		}
		for _, t := range tasks {
			lastError := ""
			if t.LastError != nil {
				lastError = *t.LastError
			}
			fmt.Printf("%s\t%s\t%s\tattempts=%d\t%s\n", t.ID, t.OrderID, t.Kind, t.AttemptCount, lastError)
		}
		return nil
	case len(args) == 2 && args[0] == "requeue":
		var id *uuid.UUID
		if args[1] != "--all" {
			parsed, err := uuid.Parse(args[1])
			if err != nil {
				return fmt.Errorf("invalid task ID: %w", err)
			}
			id = &parsed
		}
		n, err := store.RequeueDeadLetters(ctx, id)
		if err != nil {
			return err
		}
		fmt.Printf("requeued %d task(s)\n", n)
		return nil
	default:
		return errors.New(usage)
	}
}
