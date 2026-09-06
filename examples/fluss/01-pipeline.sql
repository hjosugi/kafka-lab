CREATE CATALOG fluss WITH (
    'type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss;
CREATE DATABASE IF NOT EXISTS lab;
USE lab;
CREATE TABLE orders (
    order_id BIGINT,
    customer_id STRING,
    amount_cents BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket.num' = '1');
CREATE TABLE customer_totals (
    customer_id STRING,
    amount_cents BIGINT,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('bucket.num' = '1');

SET 'table.dml-sync' = 'true';
INSERT INTO orders VALUES (1001, 'u1', 1200), (1002, 'u1', 500), (1003, 'u2', 700);
SET 'table.dml-sync' = 'false';
SET 'execution.checkpointing.interval' = '10 s';
INSERT INTO customer_totals SELECT customer_id, SUM(amount_cents) FROM orders GROUP BY customer_id;
