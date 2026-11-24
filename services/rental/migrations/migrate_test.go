package migrations

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func TestUpgradeOrderCosts(t *testing.T) {
	dsn := os.Getenv("RENTAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set RENTAL_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := t.Context()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { admin.Close() })

	name := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	schema := pgx.Identifier{name}.Sanitize()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	cfg = cfg.Copy()
	cfg.RuntimeParams["search_path"] = name
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { db.Close() })

	provider, err := goose.NewProvider(goose.DialectPostgres, db, files)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 1); err != nil {
		t.Fatalf("apply version 1: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		CREATE TABLE orders_history_p20300107 PARTITION OF orders_history
			FOR VALUES FROM ('2030-01-07 00:00:00+00') TO ('2030-01-14 00:00:00+00');

		WITH offer AS (
			INSERT INTO offers (offer_id, idempotency_id, user_id, scooter_id, zone_id, charge,
				minute_rate, unlock_fee, deposit, user_profile_fallback_used, status, created_at, expires_at)
			VALUES (gen_random_uuid(), 'offer', 'user', 'scooter', 'zone', 80, 15, 40, 0,
				false, 'CONSUMED', now(), now())
			RETURNING offer_id
		)
		INSERT INTO orders (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate,
			unlock_fee, deposit, status, created_at, closed_at, delete_after, cost)
		SELECT gen_random_uuid(), offer_id, 'user', 'scooter', 'zone', 15, 40, 0,
			'COMPLETED', now(), now(), '2030-01-08 00:00:00+00', 9223372036854775807 FROM offer;

		INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id, minute_rate,
			unlock_fee, deposit, status, created_at, closed_at, delete_after, cost, user_profile_fallback_used)
		SELECT gen_random_uuid(), gen_random_uuid(), user_id, scooter_id, zone_id, minute_rate,
			unlock_fee, deposit, status, created_at, closed_at, delete_after, cost - 1, false FROM orders;
	`)
	if err != nil {
		t.Fatalf("seed version 1: %v", err)
	}

	if _, err := provider.UpTo(ctx, 2); err != nil {
		t.Fatalf("apply payload migration: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE orders SET payload = set_byte(payload, 0, 42);
		UPDATE orders_history SET payload = set_byte(payload, 0, 43);
	`); err != nil {
		t.Fatalf("seed payloads: %v", err)
	}

	if err := Up(ctx, db); err != nil {
		t.Fatalf("upgrade version 2: %v", err)
	}
	var currentByte, archivedByte int
	err = db.QueryRowContext(ctx, `
		SELECT (SELECT get_byte(payload, 0) FROM orders),
			(SELECT get_byte(payload, 0) FROM orders_history)
	`).Scan(&currentByte, &archivedByte)
	if err != nil || currentByte != 42 || archivedByte != 43 {
		t.Fatalf("payloads after upgrade: current=%d archived=%d err=%v", currentByte, archivedByte, err)
	}

	requireCosts := func(wantCurrent, wantArchived string) {
		t.Helper()
		var current, archived string
		err := db.QueryRowContext(ctx, `
			SELECT (SELECT cost::text FROM orders), (SELECT cost::text FROM orders_history)
		`).Scan(&current, &archived)
		if err != nil || current != wantCurrent || archived != wantArchived {
			t.Fatalf("current cost %q, archived cost %q, want %q and %q, error %v",
				current, archived, wantCurrent, wantArchived, err)
		}
	}
	requireCosts("9223372036854775807", "9223372036854775806")

	if _, err := db.ExecContext(ctx, `
		UPDATE orders SET cost = 9223372036854775808;
		UPDATE orders_history SET cost = 9223372036854775809;
	`); err != nil {
		t.Fatalf("write large costs after upgrade: %v", err)
	}
	requireCosts("9223372036854775808", "9223372036854775809")
}
