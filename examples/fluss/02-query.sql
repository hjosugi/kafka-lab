CREATE CATALOG fluss WITH (
    'type' = 'fluss', 'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss;
USE lab;
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
-- Without lake tiering, batch reads must be primary-key point queries.
SELECT * FROM customer_totals WHERE customer_id = 'u1';
SELECT * FROM customer_totals WHERE customer_id = 'u2';
