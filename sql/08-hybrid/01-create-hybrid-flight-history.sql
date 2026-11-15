-- Append-only history used to prove the hybrid read: an old batch that will exist only in
-- Iceberg (after its Kafka records are deleted) and a new batch that exists in both.
-- Same shape as flight_history (sql/05-history), so it reads like the quick start table.
CREATE TABLE hybrid_flight_history (
  flight_id STRING,
  flight_date DATE,
  delay_minutes INT,
  affected_passengers INT
);
