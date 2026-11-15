-- lab:mode=snapshot
-- Row counts per risk class, to confirm a generated dataset has fully landed.
SELECT risk, COUNT(*) AS passengers
FROM bench_passenger_state
GROUP BY risk;
