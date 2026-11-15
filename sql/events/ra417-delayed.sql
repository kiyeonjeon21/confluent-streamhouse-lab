-- RA417 now lands at 14:50, almost two hours late. passenger_state must recompute
-- P-001 and P-002 to AT_RISK without any change to its definition.
-- Adapted from the Confluent Streamhouse quick start, Step 4.
INSERT INTO flight_status VALUES
  ('RA417', 'ORD', 'SFO', TIMESTAMP '2026-11-04 13:00:00',
    TIMESTAMP '2026-11-04 14:50:00', 'DELAYED');
