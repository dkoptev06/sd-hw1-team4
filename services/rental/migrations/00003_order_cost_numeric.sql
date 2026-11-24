-- +goose Up
ALTER TABLE orders ALTER COLUMN cost TYPE numeric,
    ADD CONSTRAINT orders_cost_whole CHECK (cost >= 0 AND cost < 'Infinity'::numeric AND scale(cost) = 0);

-- A type change on the partitioned parent also updates its attached partitions.
ALTER TABLE orders_history ALTER COLUMN cost TYPE numeric,
    ADD CONSTRAINT orders_history_cost_whole CHECK (cost >= 0 AND cost < 'Infinity'::numeric AND scale(cost) = 0);

-- +goose Down
ALTER TABLE orders_history DROP CONSTRAINT orders_history_cost_whole,
    ALTER COLUMN cost TYPE bigint USING cost::bigint;
ALTER TABLE orders DROP CONSTRAINT orders_cost_whole,
    ALTER COLUMN cost TYPE bigint USING cost::bigint;
