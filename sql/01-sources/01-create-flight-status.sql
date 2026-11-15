-- Latest status per flight. Upsert on a compacted topic, so the topic keeps one value per key.
-- Adapted from the Confluent Streamhouse quick start, Step 2.
CREATE TABLE flight_status (
  flight_id STRING,
  origin STRING,
  destination STRING,
  scheduled_time TIMESTAMP(3),
  estimated_time TIMESTAMP(3),
  status STRING,
  PRIMARY KEY (flight_id) NOT ENFORCED
) DISTRIBUTED BY (flight_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'kafka.cleanup-policy' = 'compact');
