-- lab:mode=snapshot
-- The upsert table holds one row per flight, however many events were written.
SELECT flight_id, origin, destination, estimated_time, status
FROM flight_status;
