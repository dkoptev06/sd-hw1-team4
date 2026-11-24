-- +goose Up
-- Keep the backfill and trigger installation atomic with concurrent archive writes.
LOCK TABLE orders_history IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE order_history_locator (
    order_id uuid PRIMARY KEY,
    offer_id uuid NOT NULL UNIQUE,
    delete_after timestamptz NOT NULL,
    partition_oid regclass NOT NULL
);

INSERT INTO order_history_locator (order_id, offer_id, delete_after, partition_oid)
SELECT order_id, offer_id, delete_after, tableoid::regclass FROM orders_history;

CREATE INDEX order_history_locator_partition ON order_history_locator (partition_oid, order_id);
ALTER TABLE history_partition_cleanup ADD COLUMN locator_cursor uuid;

-- +goose StatementBegin
CREATE FUNCTION sync_order_history_locator() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        DELETE FROM order_history_locator WHERE order_id = OLD.order_id AND partition_oid = TG_RELID::regclass;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        INSERT INTO order_history_locator (order_id, offer_id, delete_after, partition_oid)
        VALUES (NEW.order_id, NEW.offer_id, NEW.delete_after, TG_RELID::regclass);
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER orders_history_locator_sync
    AFTER INSERT OR DELETE OR UPDATE OF order_id, offer_id, delete_after ON orders_history
    FOR EACH ROW EXECUTE FUNCTION sync_order_history_locator();

-- +goose Down
DROP TRIGGER orders_history_locator_sync ON orders_history;
DROP FUNCTION sync_order_history_locator();
ALTER TABLE history_partition_cleanup DROP COLUMN locator_cursor;
DROP TABLE order_history_locator;
