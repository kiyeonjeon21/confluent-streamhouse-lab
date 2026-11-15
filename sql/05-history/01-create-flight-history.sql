-- Append-only history of past flights. Tableflow materializes this topic as an Iceberg table.
-- Adapted from the Confluent Streamhouse quick start, Step 7.
CREATE TABLE flight_history (
  flight_id STRING,
  flight_date DATE,
  delay_minutes INT,
  affected_passengers INT
);
