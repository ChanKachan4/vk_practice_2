-- Silver -> Gold: daily report. Recomputes the last 3 days, so late events correct earlier days.
MERGE INTO iceberg.gold.daily_device_report AS t
USING (
  SELECT
    event_date AS report_date,
    device_id,
    count(*) AS events_count,
    avg("value") AS avg_value,
    min("value") AS min_value,
    max("value") AS max_value,
    CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS computed_at
  FROM iceberg.silver.events
  WHERE event_date >= current_date - INTERVAL '3' DAY
  GROUP BY event_date, device_id
) AS s
ON t.report_date = s.report_date AND t.device_id = s.device_id
WHEN MATCHED THEN
  UPDATE SET events_count = s.events_count, avg_value = s.avg_value,
             min_value = s.min_value, max_value = s.max_value, computed_at = s.computed_at
WHEN NOT MATCHED THEN
  INSERT (report_date, device_id, events_count, avg_value, min_value, max_value, computed_at)
  VALUES (s.report_date, s.device_id, s.events_count, s.avg_value, s.min_value, s.max_value, s.computed_at);
