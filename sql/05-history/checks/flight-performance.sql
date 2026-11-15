-- lab:mode=snapshot
-- Reads the Iceberg snapshot and the newest Kafka records together when Tableflow is on.
-- Adapted from the Confluent Streamhouse quick start, Step 7.
SELECT flight_id,
       COUNT(*) AS flights,
       SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END) AS delayed,
       SUM(affected_passengers) AS impacted
FROM flight_history
GROUP BY flight_id;
