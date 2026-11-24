package app

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/maintenance"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/task"
)

func runMaintenance(ctx context.Context, cfg settings, pool *pgxpool.Pool, log *slog.Logger) error {
	registerPoolMetrics(pool)
	tasks := task.NewService(pool, cfg.Tasks)
	m := maintenance.NewService(pool, tasks, cfg.Maintenance, log)
	log.Info("maintenance started", "interval", cfg.Maintenance.Interval.String())
	return runWithOpsServer(ctx, cfg.HTTPAddr, pool, log, m.Run)
}
