-- Generate historical GET targets in an isolated, migrated acceptance database.
-- psql -XqAt -v rows=53000 -v expired=12000 < tools/loadtest/scripts/seed-history.sql > build/history.ids
-- Only retained UUIDs go to stdout (sorted in C-locale text order for O(1)-memory
-- verification). The optional second cohort already expired in a completed UTC
-- week so the service can detach and drop its whole partition.
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
SELECT n, 'retained'::text AS kind, gen_random_uuid() AS order_id,
       'load-history-retained-' || (1 + (n % 40000)) AS scooter_id,
       p.at + interval '12 hours' + (n % 53) * interval '7 days' AS delete_after
FROM fixture_params p CROSS JOIN LATERAL generate_series(0, p.retained - 1) n
UNION ALL
SELECT n, 'expired'::text AS kind, gen_random_uuid() AS order_id,
       'load-history-expired-' || (1 + (n % 40000)) AS scooter_id,
       date_trunc('week', p.at) - interval '3 days' AS delete_after
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

-- Realistic mix of zones, tariffs, ride durations (4-6 min), and terminal
-- statuses (96% COMPLETED, 2% CANCELLED, 2% START_FAILED). The 848-byte
-- per-order payload matches the ~1 KiB inline row size of live orders
-- without triggering TOAST compression or changing the GET response size.
WITH tariffs(zone_idx, zone_id, base_rate, price_unlock, full_deposit) AS (
    VALUES (0, 'zone-1', 10, 50, 300),
           (1, 'zone-2', 12, 40, 250),
           (2, 'zone-3',  8, 30, 200),
           (3, 'zone-4', 15, 60, 350)
), priced AS (
    SELECT f.n, f.order_id, f.scooter_id, f.delete_after,
           t.zone_id,
           floor(floor(t.base_rate * 1.25) * CASE WHEN ((f.n::bigint * 37) % 101) < 28 THEN 0.8 ELSE 1 END)::bigint AS minute_rate,
           CASE WHEN ((f.n / 4) % 4) = 0 THEN 0 ELSE t.price_unlock END::bigint AS unlock_fee,
           CASE WHEN ((f.n / 4) % 3) = 0 THEN 0 ELSE t.full_deposit END::bigint AS deposit,
           CASE
               WHEN f.n % 50 = 49 THEN 'START_FAILED'
               WHEN f.n % 50 = 48 THEN 'CANCELLED'
               ELSE 'COMPLETED'
           END AS status,
           (240 + (f.n % 121))::integer AS duration_s,
           f.delete_after - interval '8760 hours' AS closed_at
    FROM fixture_orders f
    JOIN tariffs t ON t.zone_idx = f.n % 4
)
INSERT INTO orders_history (order_id, offer_id, user_id, scooter_id, zone_id,
    minute_rate, unlock_fee, deposit, status, created_at, started_at,
    finish_time, closed_at, delete_after, cost, user_profile_fallback_used, payload)
SELECT order_id,
       gen_random_uuid(),
       'user-' || (1 + (n % 40000)),
       scooter_id,
       zone_id,
       minute_rate,
       unlock_fee,
       deposit,
       status,
       CASE status
           WHEN 'START_FAILED' THEN closed_at - interval '2 minutes'
           WHEN 'CANCELLED' THEN closed_at - interval '3 seconds'
           ELSE closed_at - make_interval(secs => duration_s + 1)
       END AS created_at,
       CASE status
           WHEN 'START_FAILED' THEN NULL
           WHEN 'CANCELLED' THEN closed_at - interval '2 seconds'
           ELSE closed_at - make_interval(secs => duration_s)
       END AS started_at,
       CASE status
           WHEN 'START_FAILED' THEN NULL
           ELSE closed_at
       END AS finish_time,
       closed_at,
       delete_after,
       CASE status
           WHEN 'COMPLETED' THEN unlock_fee + (duration_s * minute_rate) / 60
           ELSE 0
       END AS cost,
       false,
       decode(repeat(md5(order_id::text), 53), 'hex')
FROM priced;

SELECT order_id FROM fixture_orders WHERE kind = 'retained' ORDER BY order_id::text COLLATE "C";
COMMIT;

-- Give the planner statistics after the bulk import.
ANALYZE orders_history;
