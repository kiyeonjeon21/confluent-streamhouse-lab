-- Experiment E6: evolve the bench materialized table in place with a stricter filter.
-- The only change from sql/07-bench is the WHERE clause, which drops passengers with no
-- connecting flight. In upsert mode Flink emits no delete for keys the new query stops
-- producing, so the documented expectation is that those rows stay downstream ("zombie"
-- keys): docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html
-- There is no DISTRIBUTED BY clause: repeating the original one, unchanged, failed on
-- 2026-11-15 with "Changing distribution of materialized table is not supported."
CREATE OR ALTER MATERIALIZED TABLE bench_passenger_state (
  PRIMARY KEY (passenger_id) NOT ENFORCED
)
WITH ('changelog.mode' = 'upsert')
AS SELECT
  p.passenger_id,
  p.inbound_flight_id,
  p.connecting_flight_id,
  COALESCE(onward.destination, inbound.destination) AS final_destination,
  inbound.estimated_time AS inbound_arrival,
  TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time)
    AS connection_minutes,
  CASE
    WHEN p.connecting_flight_id IS NULL THEN 'NO_CONNECTION'
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time,
                       onward.estimated_time) < 45 THEN 'AT_RISK'
    ELSE 'OK'
  END AS risk
FROM bench_passenger_itineraries p
JOIN bench_flight_status inbound
  ON p.inbound_flight_id = inbound.flight_id
LEFT JOIN bench_flight_status onward
  ON p.connecting_flight_id = onward.flight_id
WHERE p.connecting_flight_id IS NOT NULL;
