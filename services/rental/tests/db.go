// Package tests provides shared fixtures for Rental's integration and functional tests.
package tests

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/migrations"
)

// NewDB creates a fresh schema with the Rental migrations applied.
// The test is skipped unless RENTAL_TEST_DATABASE_URL is set.
func NewDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("RENTAL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set RENTAL_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	name := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	schema := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	return pool
}

// ExecSQL runs a fixture statement.
func ExecSQL(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
