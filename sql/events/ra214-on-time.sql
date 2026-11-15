-- Restores RA214 to its seeded on-time state, so experiment E5 can replay its delay.
-- Not part of the upstream quick start.
INSERT INTO flight_status VALUES
  ('RA214', 'SFO', 'SEA', TIMESTAMP '2026-11-04 15:30:00',
    TIMESTAMP '2026-11-04 15:30:00', 'ON_TIME');
