-- Run after client and worker drain, on a dedicated load-test database.
-- Times come from the driver's JSON. Include archived orders in every invariant.
WITH all_orders AS (
    SELECT o.order_id, o.offer_id, o.scooter_id, o.user_id, o.zone_id, o.deposit, o.status, o.created_at, o.started_at,
           o.finish_time, o.closed_at, o.cost, o.unlock_fee, o.minute_rate, pg_column_size(o) AS row_bytes,
           f.user_profile_fallback_used
    FROM orders o LEFT JOIN offers f USING (offer_id)
    UNION ALL
    SELECT order_id, offer_id, scooter_id, user_id, zone_id, deposit, status, created_at, started_at,
           finish_time, closed_at, cost, unlock_fee, minute_rate, pg_column_size(h) AS row_bytes,
           h.user_profile_fallback_used
    FROM orders_history h
), cohort AS (
    SELECT *, lag(closed_at) OVER (PARTITION BY scooter_id ORDER BY created_at, order_id) AS previous_close
    FROM all_orders WHERE created_at >= :'run_start' AND created_at < :'run_end'
), seeded AS (
    -- This profile runs against the default Source catalog. Check the offer
    -- terms independently of the amounts persisted by Rental.
    SELECT c.*, substring(scooter_id FROM '^scooter-([0-9]+)$')::int AS scooter_number,
           substring(user_id FROM '^user-([1-9][0-9]{0,17})$')::bigint AS user_number
    FROM cohort c
), tariffs(zone_number, base_rate, unlock, full_deposit) AS (
    VALUES (0,10,50,300), (1,12,40,250), (2,8,30,200), (3,15,60,350)
), summary AS (
    SELECT count(*) AS trips,
           count(*) FILTER (WHERE status IN ('CANCELLED','COMPLETED')) AS finished,
           count(*) FILTER (WHERE status = 'STARTING') AS still_starting,
           count(*) FILTER (WHERE status = 'START_FAILED') AS failed_starts,
           count(*) FILTER (WHERE user_profile_fallback_used) AS fallback_orders,
           count(*) FILTER (WHERE created_at >= :'win_from' AND created_at < :'win_to') AS created_in_window,
           round(avg(row_bytes) FILTER (WHERE created_at >= :'win_from' AND created_at < :'win_to'), 1) AS mean_order_row_bytes_in_window,
           count(*) FILTER (WHERE status = 'START_FAILED' AND created_at >= :'win_from' AND created_at < :'win_to') AS failed_starts_in_window,
           count(*) FILTER (WHERE started_at >= :'win_from' AND started_at < :'win_to') AS activated_in_window,
           count(*) FILTER (WHERE finish_time >= :'win_from' AND finish_time < :'win_to') AS finished_in_window,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(epoch FROM (started_at-created_at))*1000)
               FILTER (WHERE started_at >= :'win_from' AND started_at < :'win_to') AS activation_p95_ms,
           count(*) FILTER (WHERE t.zone_number IS NULL OR user_number IS NULL OR user_profile_fallback_used IS NULL OR
               zone_id <> 'zone-' || (t.zone_number+1) OR
               minute_rate <> floor(floor(t.base_rate*1.25) *
                   CASE WHEN (scooter_number*37)%101 < 28 THEN 0.8 ELSE 1 END) OR
               unlock_fee <> CASE WHEN NOT user_profile_fallback_used AND user_number%4=0 THEN 0 ELSE t.unlock END OR
               deposit <> CASE WHEN NOT user_profile_fallback_used AND user_number%3=0 THEN 0 ELSE t.full_deposit END) AS wrong_terms,
           count(*) - count(DISTINCT offer_id) AS reused_offers,
           count(*) FILTER (WHERE previous_close > created_at) AS overlapping_rentals,
           count(*) FILTER (WHERE status IN ('COMPLETED','CANCELLED') AND
               (started_at IS NULL OR finish_time < started_at OR cost IS DISTINCT FROM
               CASE WHEN finish_time - started_at < interval '5 seconds' THEN 0
                    ELSE unlock_fee + trunc(floor(EXTRACT(epoch FROM finish_time-started_at)) * minute_rate / 60) END
               OR (status = 'CANCELLED') IS DISTINCT FROM (finish_time-started_at < interval '5 seconds'))) AS wrong_bills,
           (SELECT pg_database_size(current_database())) AS database_bytes,
           (SELECT count(*) FROM pg_inherits WHERE inhparent='orders_history'::regclass) AS history_partitions,
           (SELECT coalesce(sum(pg_table_size(inhrelid)), 0) FROM pg_inherits
             WHERE inhparent='orders_history'::regclass) AS history_table_bytes,
           (SELECT coalesce(sum(pg_indexes_size(inhrelid)), 0) FROM pg_inherits
             WHERE inhparent='orders_history'::regclass) AS history_index_bytes,
           (SELECT count(*) FROM tasks WHERE status IN ('PENDING','DEAD_LETTER')) AS outstanding_tasks
    FROM seeded LEFT JOIN tariffs t ON t.zone_number = scooter_number % 4
)
SELECT row_to_json(summary) FROM summary;
