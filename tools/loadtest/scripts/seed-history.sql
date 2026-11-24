-- Generate historical GET targets in an isolated, migrated acceptance database.
-- psql -XqAt -v rows=53000 -v expired=12000 < tools/loadtest/scripts/seed-history.sql > build/history.ids
-- Only retained UUIDs go to stdout. The optional second cohort already expired
-- in a completed UTC week so the service can detach and drop its whole partition.
\set ON_ERROR_STOP on
\if :{?rows}
\else
\set rows 53000
\endif
\if :{?expired}
\else
\set expired 0
\endif

BEGIN;
SET LOCAL TIME ZONE 'UTC';
CREATE TEMP TABLE fixture_params (
    retained integer CHECK (retained > 0),
    expired integer CHECK (expired >= 0),
    at timestamptz NOT NULL
) ON COMMIT DROP;
INSERT INTO fixture_params VALUES (:'rows'::integer, :'expired'::integer, now());

-- 0..52 weeks span the year. All retained orders have already closed and
-- their earliest expiry is 12 hours away, even during the full-length run.
CREATE TEMP TABLE fixture_orders ON COMMIT DROP AS
SELECT n, gen_random_uuid() AS order_id, 'load-history-retained' AS scooter_id,
       p.at + interval '12 hours' + (n % 53) * interval '7 days' AS delete_after
FROM fixture_params p CROSS JOIN LATERAL generate_series(0, p.retained - 1) n
UNION ALL
SELECT n, gen_random_uuid(), 'load-history-expired',
       date_trunc('week', p.at) - interval '3 days'
FROM fixture_params p CROSS JOIN LATERAL generate_series(0, p.expired - 1) n;

DO $$
DECLARE
    week timestamptz;
    child text;
BEGIN
    FOR week IN SELECT DISTINCT date_trunc('week', delete_after) FROM fixture_orders ORDER BY 1 LOOP
        child := 'orders_history_p' || to_char(week, 'YYYYMMDD');
        IF to_regclass(child) IS NULL THEN
            EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF orders_history FOR VALUES FROM (%L) TO (%L)',
                           child, week, week + interval '7 days');
        END IF;
    END LOOP;
END $$;

-- The long synthetic user identifier gives the requested ~1 KiB physical row
-- without modifying the production schema or increasing the GET response size.
INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id,
    minute_rate, unlock_fee, deposit, status, created_at, started_at,
    finish_time, closed_at, delete_after, cost, user_profile_fallback_used)
SELECT order_id, gen_random_uuid(), 'load-' || repeat(md5(order_id::text), 26), scooter_id,
       'zone-' || (1 + n % 4), 60, 50, 300, 'COMPLETED',
       delete_after - interval '365 days 5 minutes 1 second',
       delete_after - interval '365 days 5 minutes', delete_after - interval '365 days',
       delete_after - interval '365 days', delete_after, 350, false
FROM fixture_orders;
SELECT order_id FROM fixture_orders WHERE scooter_id = 'load-history-retained' ORDER BY n;
COMMIT;

-- Give the planner statistics after the bulk import.
ANALYZE orders_history;
