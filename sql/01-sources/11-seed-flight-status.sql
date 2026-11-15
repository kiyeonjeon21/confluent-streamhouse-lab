-- Inbound RA417 from Chicago and four onward SFO to SEA flights, all on time.
-- Adapted from the Confluent Streamhouse quick start, Step 2.
INSERT INTO flight_status VALUES
  ('RA417', 'ORD', 'SFO', TIMESTAMP '2026-11-04 13:00:00',
    TIMESTAMP '2026-11-04 13:00:00', 'ON_TIME'),
  ('RA210', 'SFO', 'SEA', TIMESTAMP '2026-11-04 14:00:00',
    TIMESTAMP '2026-11-04 14:00:00', 'ON_TIME'),
  ('RA214', 'SFO', 'SEA', TIMESTAMP '2026-11-04 15:30:00',
    TIMESTAMP '2026-11-04 15:30:00', 'ON_TIME'),
  ('RA226', 'SFO', 'SEA', TIMESTAMP '2026-11-04 17:45:00',
    TIMESTAMP '2026-11-04 17:45:00', 'ON_TIME'),
  ('RA232', 'SFO', 'SEA', TIMESTAMP '2026-11-05 07:15:00',
    TIMESTAMP '2026-11-05 07:15:00', 'ON_TIME');
