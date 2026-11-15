-- The new batch for the hybrid-read experiment, written after the old batch's Kafka
-- records are deleted. It is the Kafka tail of the hybrid read.
INSERT INTO hybrid_flight_history VALUES
  ('NEW-1', DATE '2026-11-14', 25, 12),
  ('NEW-2', DATE '2026-11-15', 5, 0);
