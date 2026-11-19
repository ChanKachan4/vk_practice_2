SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'live-writer';
SET 'table.exec.source.idle-timeout' = '10 s';

CREATE TEMPORARY TABLE kafka_live (
  msg STRING,
  event_time AS COALESCE(
    TRY_CAST(REPLACE(SUBSTR(JSON_VALUE(msg, '$.event_ts'), 1, 19), 'T', ' ') AS TIMESTAMP(3)),
    TIMESTAMP '1970-01-01 00:00:00.000'),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw-events',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'live-writer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TEMPORARY VIEW events_parsed AS
SELECT
  JSON_VALUE(msg, '$.event_id') AS event_id,
  JSON_VALUE(msg, '$.device_id') AS device_id,
  JSON_VALUE(msg, '$.value' RETURNING DOUBLE) AS `value`,
  event_time
FROM kafka_live
WHERE msg IS JSON;

CREATE TEMPORARY TABLE live_agg_sink (
  device_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  events_count BIGINT,
  min_value DOUBLE,
  max_value DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'live-agg',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

INSERT INTO live_agg_sink
SELECT
  device_id,
  window_start,
  window_end,
  COUNT(DISTINCT event_id) AS events_count,
  MIN(`value`) AS min_value,
  MAX(`value`) AS max_value
FROM TABLE(TUMBLE(TABLE events_parsed, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY device_id, window_start, window_end;
