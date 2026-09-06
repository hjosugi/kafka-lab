CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR,
    event_time TIMESTAMPTZ,
    kind VARCHAR,
    service VARCHAR,
    level VARCHAR,
    user_id VARCHAR,
    category VARCHAR,
    action VARCHAR,
    latency_ms INT
) WITH (
    connector = 'kafka',
    topic = 'lab.events',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Producers must reuse an immutable payload for the same event_id.
-- Exact duplicates are collapsed before any aggregation.
CREATE MATERIALIZED VIEW IF NOT EXISTS events AS
SELECT DISTINCT event_id, event_time, kind, service, level,
                user_id, category, action, latency_ms
FROM raw_events;

CREATE MATERIALIZED VIEW IF NOT EXISTS log_counts AS
SELECT service, level, COUNT(*) AS total
FROM events WHERE kind = 'log' GROUP BY service, level;

CREATE MATERIALIZED VIEW IF NOT EXISTS recommendation_scores AS
SELECT user_id, category,
       SUM(CASE action WHEN 'buy' THEN 5 WHEN 'cart' THEN 3 ELSE 1 END) AS score
FROM events WHERE kind = 'interaction' GROUP BY user_id, category;

-- Retrospective event-time buckets; no watermark finalization is claimed.
CREATE MATERIALIZED VIEW IF NOT EXISTS service_minutes AS
SELECT service, DATE_TRUNC('minute', event_time) AS minute,
       COUNT(*) AS requests,
       SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS errors,
       AVG(latency_ms) AS avg_latency_ms
FROM events WHERE kind = 'log'
GROUP BY service, DATE_TRUNC('minute', event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS alerts AS
SELECT * FROM service_minutes
WHERE requests >= 3 AND errors * 1.0 / requests >= 0.5;
