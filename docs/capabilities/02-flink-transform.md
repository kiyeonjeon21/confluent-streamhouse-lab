# Capability 2: Flink SQL transformations and joins

## Architecture and purpose

The quick start's only join lives inside the `passenger_state` materialized table.
This capability runs the same join once as a bounded snapshot query, to separate two things that are easy to conflate: the SQL logic, and keeping its result continuously up to date.
Capability 3 adds only the second.

The query joins each itinerary to its inbound flight (inner join) and connecting flight (left join), and derives the final destination and connection minutes.

## Deployment steps

Nothing to deploy; it reads the capability 1 tables.

## SQL, configuration, and API examples

- [`sql/02-transform/checks/connections.sql`](../../sql/02-transform/checks/connections.sql)
- Snapshot mode is the statement property `sql.snapshot.mode = now`; the statement runs in batch mode and completes.

## Test procedure

```sh
uv run lab check transform
scripts/verify-live capability_2
```

## Expected versus actual results

Expected: `P-001` to `SEA` with 60 connection minutes, `P-002` to `SEA` with 150, `P-003` ending at `SFO` with no connection minutes.

Actual (2026-11-15, `uv run lab check transform` and `scripts/verify-live capability_2`): **matches**.

```
passenger_id  inbound_flight_id  connecting_flight_id  final_destination  connection_minutes
P-001         RA417              RA210                 SEA                60
P-002         RA417              RA214                 SEA                150
P-003         RA417              NULL                  SFO                NULL
```

## Issues, limitations, and lessons learned

- The bounded join took 9.7 s wall clock from submit to deleted statement, including results paging.
- Running the same join continuously (capability 3) makes Flink warn that it is a high-state operator without a TTL. As a bounded query it carries no such cost, which is the clearest argument for keeping exploratory joins in snapshot mode.
