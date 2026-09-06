SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.dir' = 'file:///checkpoints';
SET 'table.exec.source.idle-timeout' = '10 s';

CREATE TABLE logs (
    event_id STRING,
    event_time TIMESTAMP_LTZ(3),
    kind STRING,
    service STRING,
    level STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'lab.events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'lab-flink-windows',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
CREATE TABLE window_output (
    service STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    requests BIGINT,
    errors BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'lab.flink.windows',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- This baseline counts physical records, including duplicates.
INSERT INTO window_output
SELECT service, window_start, window_end, COUNT(*),
       SUM(CASE WHEN level = 'ERROR' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END)
FROM TABLE(TUMBLE(TABLE logs, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE kind = 'log'
GROUP BY service, window_start, window_end;
