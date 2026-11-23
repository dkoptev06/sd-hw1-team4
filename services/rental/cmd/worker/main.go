package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/app/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx); err != nil {
		slog.Error("rental worker stopped", "error", err)
		os.Exit(1)
	}
}
