-- lab:mode=snapshot
-- The join and transformation that capability 3 keeps running, executed once as a bounded query.
-- Comparing this result with passenger_state shows that the materialized table adds
-- continuous maintenance, not different logic.
SELECT
  p.passenger_id,
  inbound.flight_id AS inbound_flight_id,
  onward.flight_id AS connecting_flight_id,
  COALESCE(onward.destination, inbound.destination) AS final_destination,
  TIMESTAMPDIFF(MINUTE, inbound.estimated_time, onward.estimated_time) AS connection_minutes
FROM passenger_itineraries p
JOIN flight_status inbound
  ON p.inbound_flight_id = inbound.flight_id
LEFT JOIN flight_status onward
  ON p.connecting_flight_id = onward.flight_id;
