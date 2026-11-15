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

Actual: **not run yet**.

## Issues, limitations, and lessons learned

To record after the first run:

- Snapshot query start-up time against a tiny dataset, which bounds how cheap a "quick check" is.
- Whether the regular (unbounded-state) join here would need a state TTL if it ran as a streaming query.
