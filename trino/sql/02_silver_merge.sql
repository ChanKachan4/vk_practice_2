-- Bronze -> Silver. Idempotent: re-running does not create duplicates.
-- Incremental: only recent bronze partitions are scanned (late events up to 3 days old).
-- A duplicate that arrives later than the window is still ignored: MERGE matches existing event_id.
MERGE INTO iceberg.silver.events AS t
USING (
  SELECT event_id, device_id, event_ts, event_date, "value", ingest_ts
  FROM (
    SELECT
      event_id,
      device_id,
      CAST(from_iso8601_timestamp(event_ts) AS TIMESTAMP(6) WITH TIME ZONE) AS event_ts,
      CAST(at_timezone(from_iso8601_timestamp(event_ts), 'UTC') AS DATE) AS event_date,
      "value",
      CAST(kafka_ts AS TIMESTAMP(6) WITH TIME ZONE) AS ingest_ts,
      row_number() OVER (
        PARTITION BY event_id
        ORDER BY kafka_ts, kafka_partition, kafka_offset
      ) AS rn
    FROM iceberg.bronze.events
    WHERE ingest_date >= current_date - INTERVAL '3' DAY
      AND event_id IS NOT NULL
      AND device_id IS NOT NULL
      AND event_ts IS NOT NULL
      AND "value" IS NOT NULL
  )
  WHERE rn = 1
) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN
  INSERT (event_id, device_id, event_ts, event_date, "value", ingest_ts)
  VALUES (s.event_id, s.device_id, s.event_ts, s.event_date, s."value", s.ingest_ts);
