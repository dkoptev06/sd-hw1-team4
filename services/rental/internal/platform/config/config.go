// Package config loads the Rental process configuration from its environment.
package config

import (
	"errors"
	"os"
)

type Config struct {
	DatabaseURL string
}

// Load reads and validates the process settings.
func Load() (Config, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	return Config{DatabaseURL: url}, nil
}
