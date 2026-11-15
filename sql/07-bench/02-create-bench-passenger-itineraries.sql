-- Phase 4 copy of passenger_itineraries.
CREATE TABLE bench_passenger_itineraries (
  passenger_id STRING,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  PRIMARY KEY (passenger_id) NOT ENFORCED
) DISTRIBUTED BY (passenger_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'kafka.cleanup-policy' = 'compact');
