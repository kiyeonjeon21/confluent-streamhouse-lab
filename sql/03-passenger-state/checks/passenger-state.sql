-- lab:mode=snapshot
-- Adapted from the Confluent Streamhouse quick start, Step 3.
SELECT passenger_id, connecting_flight_id, inbound_arrival,
       connection_minutes, risk
FROM passenger_state;
