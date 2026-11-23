// Package testdb gives integration tests an isolated, migrated PostgreSQL schema.
// Tests are skipped unless RENTAL_TEST_DATABASE_URL is set.
package testdb

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/dkoptev06/sd-hw1-team4/services/rental/internal/storage"
	"github.com/dkoptev06/sd-hw1-team4/services/rental/migrations"
)

// New creates a fresh schema with the Rental migrations applied and returns a store on it.
func New(t *testing.T) *storage.Store {
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
	return storage.New(pool)
}

// Exec runs a fixture statement.
func Exec(t *testing.T, store *storage.Store, sql string, args ...any) {
	t.Helper()
	if _, err := store.Pool().Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
