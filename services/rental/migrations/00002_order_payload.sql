-- +goose Up
-- Add an 848-byte inline payload column to orders and orders_history so each
-- persisted order record occupies ~1 KiB on PostgreSQL heap pages (matching
-- the Z = 1 KB requirement without exceeding TOAST_TUPLE_THRESHOLD).
ALTER TABLE orders
    ADD COLUMN payload bytea NOT NULL DEFAULT decode(repeat('00', 848), 'hex');

ALTER TABLE orders_history
    ADD COLUMN payload bytea NOT NULL DEFAULT decode(repeat('00', 848), 'hex');

-- +goose Down
ALTER TABLE orders_history DROP COLUMN payload;
ALTER TABLE orders DROP COLUMN payload;
