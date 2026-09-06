CREATE TABLE logs (
    event_id TEXT,
    event_time TEXT,
    kind TEXT,
    service TEXT,
    level TEXT,
    latency_ms INT
) WITH (
    connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = 'kafka:9092', topic = 'lab.events'
);
CREATE TABLE errors WITH (
    connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = 'kafka:9092', topic = 'lab.arroyo.alerts'
);
INSERT INTO errors
SELECT event_id, service, latency_ms FROM logs
WHERE kind = 'log' AND level = 'ERROR';
