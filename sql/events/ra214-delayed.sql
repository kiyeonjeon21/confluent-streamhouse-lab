-- RA214 (SFO to SEA) now leaves at 16:00 instead of 15:30. For a passenger who lands
-- at 14:50, it becomes the first flight at least 45 minutes later, and no offered
-- flight leaves six or more hours later, so the agent's correct answer changes from
-- OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel to OFFER1=RA214;OFFER2=RA226;HOTEL=NONE.
-- Used by experiment E5. Not part of the upstream quick start.
INSERT INTO flight_status VALUES
  ('RA214', 'SFO', 'SEA', TIMESTAMP '2026-11-04 15:30:00',
    TIMESTAMP '2026-11-04 16:00:00', 'DELAYED');
