// Package storage contains all SQL of Rental. Callers own transactions through
// Store.InTx and keep the lock order offers → orders → tasks.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound means the requested row does not exist.
var ErrNotFound = errors.New("storage: not found")

type db interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Queries runs statements either on the pool or inside a transaction.
type Queries struct {
	db db
}

// Store is the entry point to the database.
type Store struct {
	Queries
	pool *pgxpool.Pool
}

// Open creates a connection pool and checks connectivity.
func Open(ctx context.Context, url string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	return New(pool), nil
}

// New wraps an existing pool; the caller keeps ownership of its lifecycle.
func New(pool *pgxpool.Pool) *Store {
	return &Store{Queries: Queries{db: pool}, pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// InTx runs fn in one READ COMMITTED transaction, committing when fn returns nil.
func (s *Store) InTx(ctx context.Context, fn func(q *Queries) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(&Queries{db: tx})
	})
}

// Now returns the database clock, the single time source for deadlines and costs.
func (q *Queries) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := q.db.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now.UTC(), err
}

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}
