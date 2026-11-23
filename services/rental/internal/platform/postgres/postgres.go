// Package postgres manages connections and transaction execution for Rental.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the SQL interface shared by the pool and a transaction.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Tx restricts operations that require an open transaction to the TxRunner callback.
type Tx interface {
	DBTX
	inTransaction()
}

type transaction struct {
	DBTX
}

func (transaction) inTransaction() {}

// OpenPool configures the pool. The caller owns its lifecycle and readiness check.
func OpenPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	return pool, nil
}

// TxRunner owns transaction startup, commit and rollback for a pool.
type TxRunner struct {
	pool *pgxpool.Pool
}

func NewTxRunner(pool *pgxpool.Pool) TxRunner {
	return TxRunner{pool: pool}
}

// WithTx executes work in one transaction and returns any work or commit error.
func (r TxRunner) WithTx(ctx context.Context, work func(Tx) error) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		return work(transaction{DBTX: tx})
	})
}
