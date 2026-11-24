-- +goose Up
-- Keep the identity of an attached partition across the two DETACH transactions.
CREATE TABLE history_partition_cleanup (
    partition_oid oid PRIMARY KEY,
    partition_name name NOT NULL UNIQUE,
    week_start timestamptz NOT NULL UNIQUE
);

-- +goose Down
DROP TABLE history_partition_cleanup;
