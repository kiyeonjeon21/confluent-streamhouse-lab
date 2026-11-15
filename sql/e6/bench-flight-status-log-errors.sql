-- Experiment E6 recovery: route records that cannot be deserialized to a dead letter
-- table instead of failing every statement that reads bench_flight_status.
-- docs.confluent.io/cloud/current/flink/how-to-guides/configure-dlq.html
ALTER TABLE bench_flight_status SET ('error-handling.mode' = 'log');
