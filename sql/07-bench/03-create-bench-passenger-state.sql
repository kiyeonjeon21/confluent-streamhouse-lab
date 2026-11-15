-- Phase 4 copy of passenger_state over the bench_ tables. The query must stay identical
-- to sql/03-passenger-state apart from table names; tests/unit/test_bench.py enforces it.
CREATE MATERIALIZED TABLE bench_passenger_state (
  PRIMARY KEY (passenger_id) NOT ENFORCED
)
DISTRIBUTED BY (passenger_id) INTO 1 BUCKETS
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
  ON p.connecting_flight_id = onward.flight_id;
