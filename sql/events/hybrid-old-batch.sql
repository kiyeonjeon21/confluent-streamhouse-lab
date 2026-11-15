-- The old batch for the hybrid-read experiment. Its Kafka records are deleted once
-- Tableflow has them, so afterwards they exist only in Iceberg.
INSERT INTO hybrid_flight_history VALUES
  ('OLD-1', DATE '2025-01-10', 15, 0),
  ('OLD-2', DATE '2025-03-21', 70, 40),
  ('OLD-3', DATE '2025-06-02', 0, 0),
  ('OLD-4', DATE '2025-09-15', 120, 88);
