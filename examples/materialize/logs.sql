CREATE CONNECTION IF NOT EXISTS lab_kafka TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL = 'PLAINTEXT');
CREATE SOURCE IF NOT EXISTS lab_events_source
FROM KAFKA CONNECTION lab_kafka (TOPIC 'lab.events');
CREATE TABLE IF NOT EXISTS lab_events FROM SOURCE lab_events_source FORMAT JSON;
CREATE MATERIALIZED VIEW IF NOT EXISTS unique_logs AS
SELECT DISTINCT data->>'event_id' AS event_id,
                data->>'service' AS service,
                data->>'level' AS level
FROM lab_events WHERE data->>'kind' = 'log';
CREATE MATERIALIZED VIEW IF NOT EXISTS log_counts AS
SELECT service, level, COUNT(*) AS total FROM unique_logs GROUP BY service, level;
SELECT * FROM log_counts ORDER BY service, level;
