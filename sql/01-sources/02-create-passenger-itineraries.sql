-- One itinerary per passenger. connecting_flight_id is NULL when the trip ends at the inbound airport.
-- Adapted from the Confluent Streamhouse quick start, Step 2.
CREATE TABLE passenger_itineraries (
  passenger_id STRING,
  inbound_flight_id STRING,
  connecting_flight_id STRING,
  PRIMARY KEY (passenger_id) NOT ENFORCED
) DISTRIBUTED BY (passenger_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'kafka.cleanup-policy' = 'compact');
