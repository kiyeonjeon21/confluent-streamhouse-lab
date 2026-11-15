-- lab:mode=snapshot
-- With Tableflow on, Flink reads older data from the Iceberg snapshot and the rest from Kafka.
SELECT flight_id, flight_date, delay_minutes, affected_passengers
FROM hybrid_flight_history;
