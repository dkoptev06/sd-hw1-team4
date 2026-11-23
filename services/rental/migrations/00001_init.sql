-- +goose Up
CREATE TABLE offers (
    offer_id uuid PRIMARY KEY,
    idempotency_id text NOT NULL UNIQUE,
    user_id text NOT NULL,
    scooter_id text NOT NULL,
    zone_id text NOT NULL,
    charge smallint NOT NULL CHECK (charge BETWEEN 0 AND 100),
    minute_rate bigint NOT NULL CHECK (minute_rate >= 0),
    unlock_fee bigint NOT NULL CHECK (unlock_fee >= 0),
    deposit bigint NOT NULL CHECK (deposit >= 0),
    user_profile_fallback_used boolean NOT NULL,
    status text NOT NULL CHECK (status IN ('AVAILABLE', 'CONSUMED')),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);
-- EXPIRED is derived from expires_at for offers that are still AVAILABLE.
CREATE INDEX offers_expiring ON offers (expires_at) WHERE status = 'AVAILABLE';

CREATE TABLE orders (
    order_id uuid PRIMARY KEY,
    offer_id uuid NOT NULL UNIQUE REFERENCES offers (offer_id),
    user_id text NOT NULL,
    scooter_id text NOT NULL,
    zone_id text NOT NULL,
    minute_rate bigint NOT NULL CHECK (minute_rate >= 0),
    unlock_fee bigint NOT NULL CHECK (unlock_fee >= 0),
    deposit bigint NOT NULL CHECK (deposit >= 0),
    status text NOT NULL CHECK (status IN ('STARTING', 'ACTIVE', 'START_FAILED', 'COMPLETED', 'CANCELLED')),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    finish_time timestamptz,
    closed_at timestamptz,
    delete_after timestamptz,
    cost bigint CHECK (cost >= 0),
    CHECK ((status IN ('START_FAILED', 'COMPLETED', 'CANCELLED')) =
           (closed_at IS NOT NULL AND delete_after IS NOT NULL AND cost IS NOT NULL))
);
-- At most one STARTING or ACTIVE order per scooter.
CREATE UNIQUE INDEX orders_occupied_scooter ON orders (scooter_id)
    WHERE status IN ('STARTING', 'ACTIVE');
-- Candidates for archiving: closed orders, oldest first.
CREATE INDEX orders_closed ON orders (closed_at) WHERE closed_at IS NOT NULL;

CREATE TABLE tasks (
    task_id uuid PRIMARY KEY,
    order_id uuid NOT NULL REFERENCES orders (order_id),
    task_type text NOT NULL CHECK (task_type IN ('START', 'SETTLE', 'VOID')),
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'COMPLETED', 'DEAD_LETTER')),
    parameters jsonb NOT NULL,
    result text,
    completion_reason text CHECK (completion_reason IN (
        'PAYMENT_REJECTED', 'DEADLINE_EXCEEDED', 'MAX_ATTEMPTS_EXCEEDED', 'CONTRACT_ERROR'
    )),
    created_at timestamptz NOT NULL,
    finished_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_id uuid,
    next_run_at timestamptz NOT NULL,
    lease_until timestamptz,
    deadline_at timestamptz,
    last_error text,
    UNIQUE (order_id, task_type),
    CHECK ((claim_id IS NULL) = (lease_until IS NULL)),
    CHECK ((deadline_at IS NOT NULL) = (task_type = 'START')),
    CHECK ((status = 'COMPLETED') = (finished_at IS NOT NULL))
);
CREATE INDEX tasks_ready ON tasks (next_run_at, task_id) WHERE status = 'PENDING';
CREATE INDEX tasks_start_deadline ON tasks (deadline_at, task_id)
    WHERE task_type = 'START' AND status = 'PENDING';
CREATE INDEX tasks_completed ON tasks (finished_at, task_id) WHERE status = 'COMPLETED';

-- Weekly partitions named orders_history_pYYYYMMDD (Monday, UTC) are managed by maintenance.
-- There is no default partition: DETACH PARTITION CONCURRENTLY does not allow one.
CREATE TABLE orders_history (
    order_id uuid NOT NULL,
    offer_id uuid NOT NULL,
    user_id text NOT NULL,
    scooter_id text NOT NULL,
    zone_id text NOT NULL,
    minute_rate bigint NOT NULL,
    unlock_fee bigint NOT NULL,
    deposit bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('START_FAILED', 'COMPLETED', 'CANCELLED')),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    finish_time timestamptz,
    closed_at timestamptz NOT NULL,
    delete_after timestamptz NOT NULL,
    cost bigint NOT NULL,
    user_profile_fallback_used boolean NOT NULL
) PARTITION BY RANGE (delete_after);
CREATE INDEX orders_history_by_order ON orders_history (order_id);
CREATE INDEX orders_history_by_offer ON orders_history (offer_id);
CREATE INDEX orders_history_retention ON orders_history (delete_after);

CREATE TABLE maintenance_leases (
    name text PRIMARY KEY,
    holder uuid NOT NULL,
    lease_until timestamptz NOT NULL
);

-- +goose Down
DROP TABLE maintenance_leases;
DROP TABLE orders_history;
DROP TABLE tasks;
DROP TABLE orders;
DROP TABLE offers;
