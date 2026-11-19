SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
SET 'pipeline.name'                    = 'bronze-writer';
CREATE CATALOG lk WITH (
  'type'                   = 'iceberg',
  'catalog-type'           = 'rest',
  'uri'                    = 'http://lakekeeper:8181/catalog',
  'warehouse'              = 'demo',
  'io-impl'                = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'            = 'http://silo:9000',
  's3.path-style-access'   = 'true',
  's3.access-key-id'       = 'minioadmin',
  's3.secret-access-key'   = 'minioadmin',
  'client.region'          = 'local-01'
);
CREATE DATABASE IF NOT EXISTS lk.bronze;
CREATE TABLE IF NOT EXISTS lk.bronze.events (
  event_id        STRING,
  device_id       STRING,
  event_ts        STRING,
  `value`         DOUBLE,
  raw_payload     STRING,
  kafka_topic     STRING,
  kafka_partition INT,
  kafka_offset    BIGINT,
  kafka_ts        TIMESTAMP_LTZ(3),
  ingest_date     DATE
) PARTITIONED BY (ingest_date);
CREATE TABLE IF NOT EXISTS lk.bronze.dead_letter (
  raw_payload     STRING,
  kafka_topic     STRING,
  kafka_partition INT,
  kafka_offset    BIGINT,
  kafka_ts        TIMESTAMP_LTZ(3)
);
CREATE TEMPORARY TABLE kafka_raw (
  msg             STRING,
  kafka_topic     STRING METADATA FROM 'topic'     VIRTUAL,
  kafka_partition INT    METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset'    VIRTUAL,
  kafka_ts        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'raw-events',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id'          = 'bronze-writer',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'raw'
);
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO lk.bronze.events
  SELECT
    JSON_VALUE(msg, '$.event_id'),
    JSON_VALUE(msg, '$.device_id'),
    JSON_VALUE(msg, '$.event_ts'),
    JSON_VALUE(msg, '$.value' RETURNING DOUBLE),
    msg, kafka_topic, kafka_partition, kafka_offset, kafka_ts,
    CAST(kafka_ts AS DATE)
  FROM kafka_raw
  WHERE msg IS JSON;
  INSERT INTO lk.bronze.dead_letter
  SELECT msg, kafka_topic, kafka_partition, kafka_offset, kafka_ts
  FROM kafka_raw
  WHERE msg IS NOT JSON;
END;
