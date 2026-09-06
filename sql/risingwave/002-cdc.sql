CREATE SOURCE IF NOT EXISTS shop_cdc WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres',
    port = '5432',
    username = 'lab',
    password = 'lab-only',
    database.name = 'shop',
    schema.name = 'public',
    slot.name = 'modern_streaming_lab',
    publication.name = 'rw_publication'
);
CREATE TABLE IF NOT EXISTS cdc_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id VARCHAR,
    amount_cents BIGINT,
    status VARCHAR,
    version BIGINT
) FROM shop_cdc TABLE 'public.orders';
CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_by_customer AS
SELECT customer_id, SUM(amount_cents) AS revenue_cents, COUNT(*) AS orders
FROM cdc_orders WHERE status = 'paid' GROUP BY customer_id;
