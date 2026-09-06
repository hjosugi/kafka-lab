CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
    status VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0)
);
ALTER TABLE orders REPLICA IDENTITY FULL;
CREATE PUBLICATION rw_publication FOR TABLE orders;
CREATE TABLE outbox (
    event_id TEXT PRIMARY KEY CHECK (event_id <> ''),
    order_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (order_id, version)
);
CREATE INDEX outbox_pending ON outbox (order_id, version) WHERE published_at IS NULL;
