-- Phase 4 copy of flight_status. Experiments write here so they never disturb the
-- quick start data that capabilities 1 to 6 and the agent depend on.
CREATE TABLE bench_flight_status (
  flight_id STRING,
  origin STRING,
  destination STRING,
  scheduled_time TIMESTAMP(3),
  estimated_time TIMESTAMP(3),
  status STRING,
  PRIMARY KEY (flight_id) NOT ENFORCED
) DISTRIBUTED BY (flight_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'kafka.cleanup-policy' = 'compact');
