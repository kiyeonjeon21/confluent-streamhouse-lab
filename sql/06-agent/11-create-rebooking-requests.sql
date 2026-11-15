-- AI_RUN_AGENT reads only append-only input, so at-risk passengers are copied
-- out of the upsert passenger_state into this append-only table.
-- Adapted from the Confluent Streamhouse quick start, Step 6.
CREATE TABLE rebooking_requests (
  passenger_id STRING,
  final_destination STRING,
  inbound_arrival TIMESTAMP(3)
);
