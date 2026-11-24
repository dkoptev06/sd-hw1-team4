package app

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/dynconfig"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/payment"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/external/source"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/order"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/platform/httpx"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/worker"
)

func runWorker(ctx context.Context, cfg settings, pool *pgxpool.Pool, log *slog.Logger) error {
	registerPoolMetrics(pool)
	sourceClient := source.NewClient(cfg.SourceURL, httpx.NewClient(), cfg.SourceTimeout, cfg.ConfigTimeout)
	configs := dynconfig.NewProvider(sourceClient, cfg.ConfigRefresh, log)
	configs.Start(ctx)
	payments := payment.NewClient(cfg.PaymentURL, httpx.NewClient(), cfg.PaymentTimeout)

	tasks := task.NewService(pool, cfg.Tasks)
	orders := order.NewService(pool, nil, tasks, nil, configs, cfg.StartTTL, cfg.HoldGrace, log)
	w := worker.New(orders, tasks, payments, cfg.Worker, log)
	log.Info("worker started", "concurrency", cfg.Worker.Concurrency)
	return runWithOpsServer(ctx, cfg.HTTPAddr, pool, log, w.Run)
}
