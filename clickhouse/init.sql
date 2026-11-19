CREATE DATABASE IF NOT EXISTS live;

-- Final table: one row per (device, minute); re-emitted windows overwrite old ones
CREATE TABLE IF NOT EXISTS live.device_minute (
  device_id    String,
  window_start DateTime64(3, 'UTC'),
  window_end   DateTime64(3, 'UTC'),
  events_count UInt64,
  min_value    Float64,
  max_value    Float64,
  updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (device_id, window_start);

-- Kafka consumer table (reads topic live-agg)
CREATE TABLE IF NOT EXISTS live.device_minute_queue (
  device_id    String,
  window_start DateTime64(3, 'UTC'),
  window_end   DateTime64(3, 'UTC'),
  events_count UInt64,
  min_value    Float64,
  max_value    Float64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'live-agg',
         kafka_group_name = 'clickhouse-live',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

-- Moves rows from the consumer table into the final table
CREATE MATERIALIZED VIEW IF NOT EXISTS live.device_minute_mv TO live.device_minute AS
SELECT device_id, window_start, window_end, events_count, min_value, max_value
FROM live.device_minute_queue;
