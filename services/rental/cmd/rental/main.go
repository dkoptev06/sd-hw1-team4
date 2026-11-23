// Command rental runs one of the Rental processes; see app.Run for the commands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "rental:", err)
		os.Exit(1)
	}
}
