-- Silver: deduplicated, typed events, partitioned by event date
CREATE TABLE IF NOT EXISTS iceberg.silver.events (
  event_id   VARCHAR,
  device_id  VARCHAR,
  event_ts   TIMESTAMP(6) WITH TIME ZONE,
  event_date DATE,
  "value"    DOUBLE,
  ingest_ts  TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['event_date']);

-- Gold: daily report per device
CREATE TABLE IF NOT EXISTS iceberg.gold.daily_device_report (
  report_date  DATE,
  device_id    VARCHAR,
  events_count BIGINT,
  avg_value    DOUBLE,
  min_value    DOUBLE,
  max_value    DOUBLE,
  computed_at  TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['report_date']);
