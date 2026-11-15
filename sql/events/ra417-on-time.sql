-- Restores RA417 to its seeded on-time state, so the delay can be replayed in experiments.
-- Not part of the upstream quick start.
INSERT INTO flight_status VALUES
  ('RA417', 'ORD', 'SFO', TIMESTAMP '2026-11-04 13:00:00',
    TIMESTAMP '2026-11-04 13:00:00', 'ON_TIME');
