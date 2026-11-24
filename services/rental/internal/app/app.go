// Package app assembles the Rental processes: api, worker, maintenance, migrate and dlq.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/postgres"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
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

	cfg, err := loadSettings()
	if err != nil {
		return err
	}
	pool, err := postgres.OpenPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	switch args[0] {
	case "api":
		return runAPI(ctx, cfg, pool, log)
	case "worker":
		return runWorker(ctx, cfg, pool, log)
	case "maintenance":
		return runMaintenance(ctx, cfg, pool, log)
	case "migrate":
		db := stdlib.OpenDBFromPool(pool)
		defer db.Close()
		return migrations.Up(ctx, db)
	case "dlq":
		return runDLQ(ctx, task.NewService(pool, task.Options{}), args[1:])
	default:
		return errors.New(usage)
	}
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

func runDLQ(ctx context.Context, service *task.Service, args []string) error {
	switch {
	case len(args) == 1 && args[0] == "list":
		tasks, err := service.ListDeadLetters(ctx)
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
		n, err := service.RequeueDeadLetters(ctx, id)
		if err != nil {
			return err
		}
		fmt.Printf("requeued %d task(s)\n", n)
		return nil
	default:
		return errors.New(usage)
	}
}
