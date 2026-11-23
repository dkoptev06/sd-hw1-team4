package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/app/api"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := api.Run(ctx, os.Args[1:]); err != nil {
		slog.Error("rental stopped", "error", err)
		os.Exit(1)
	}
}
