// Package postgres manages Rental's PostgreSQL connections and transactions.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the SQL interface shared by pools, sessions and transactions.
type DBTX interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Tx identifies a transaction whose commit and rollback belong to WithTx.
type Tx interface {
	DBTX
	inTransaction()
}

type transaction struct{ DBTX }

func (transaction) inTransaction() {}

func OpenPool(ctx context.Context, address string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(address)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	return pool, nil
}

func WithTx(ctx context.Context, db DBTX, work func(Tx) error) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		return work(transaction{DBTX: tx})
	})
}

// Now reads the database clock used for deadlines and costs.
func Now(ctx context.Context, db DBTX) (time.Time, error) {
	var now time.Time
	err := db.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now.UTC(), err
}
