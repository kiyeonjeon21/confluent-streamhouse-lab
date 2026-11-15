-- Rooms currently available at each airport hotel.
-- Adapted from the Confluent Streamhouse quick start, Step 2.
CREATE TABLE hotel_inventory (
  hotel STRING,
  available_rooms INT,
  nightly_rate DECIMAL(10, 2),
  PRIMARY KEY (hotel) NOT ENFORCED
) DISTRIBUTED BY (hotel) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert', 'kafka.cleanup-policy' = 'compact');
