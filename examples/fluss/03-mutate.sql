CREATE CATALOG fluss WITH (
    'type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss;
USE lab;
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
UPDATE orders SET amount_cents = 1800 WHERE order_id = 1001;
DELETE FROM orders WHERE order_id = 1002;
