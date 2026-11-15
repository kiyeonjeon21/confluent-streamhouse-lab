-- Three passengers on RA417: two connect at SFO, one ends there.
-- Adapted from the Confluent Streamhouse quick start, Step 2.
INSERT INTO passenger_itineraries VALUES
  ('P-001', 'RA417', 'RA210'),
  ('P-002', 'RA417', 'RA214'),
  ('P-003', 'RA417', CAST(NULL AS STRING));
