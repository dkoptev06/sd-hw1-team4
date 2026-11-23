// Package worker assembles the Rental worker process.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
)

// Run is the composition entry point for the Rental worker.
func Run(_ context.Context) error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	// TODO: Build the dependencies and start the worker loop.
	return errors.New("rental worker is not implemented")
}
