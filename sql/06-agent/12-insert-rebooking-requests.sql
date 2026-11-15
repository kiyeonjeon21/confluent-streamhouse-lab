-- lab:mode=snapshot
-- Copies the passengers who are AT_RISK right now. Expects the RA417 delay event to have run.
-- Adapted from the Confluent Streamhouse quick start, Step 6.
INSERT INTO rebooking_requests
SELECT passenger_id, final_destination, inbound_arrival
FROM passenger_state
WHERE risk = 'AT_RISK';
