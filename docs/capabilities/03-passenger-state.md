# Capability 3: continuously maintained passenger state

## Architecture and purpose

`passenger_state` is a Flink materialized table: one object that owns a table definition, its backing compacted topic, its schema subject, and the continuous query that fills it.
It keeps one row per passenger with connection minutes and a risk flag (`OK`, `AT_RISK` under 45 minutes, `NO_CONNECTION`).
When RA417 is delayed, Flink recomputes the affected rows; the definition never changes.

## Deployment steps

```sh
uv run lab apply passenger-state
```

## SQL, configuration, and API examples

- [`sql/03-passenger-state/01-create-passenger-state.sql`](../../sql/03-passenger-state/01-create-passenger-state.sql)
- Events: [`sql/events/ra417-delayed.sql`](../../sql/events/ra417-delayed.sql) and [`ra417-on-time.sql`](../../sql/events/ra417-on-time.sql), replayed with `uv run lab event <name>` under a fresh statement name each time.
- Evolution is `CREATE OR ALTER MATERIALIZED TABLE`; it discards state and reprocesses per `START_MODE` (default `RESUME_OR_FROM_BEGINNING`).

## Test procedure

```sh
uv run lab check passenger-state
uv run lab event ra417-delayed
uv run lab check passenger-state
scripts/verify-live capability_3
```

The live test first replays `ra417-on-time`, so it is repeatable.

## Expected versus actual results

Expected before the delay: `P-001 OK 60`, `P-002 OK 150`, `P-003 NO_CONNECTION NULL`.
Expected after: `P-001 AT_RISK -50`, `P-002 AT_RISK 40`, `P-003 NO_CONNECTION NULL`, within about a minute.

Actual: **not run yet**.

## Issues, limitations, and lessons learned

Known before running:

- The materialized table is a continuous statement and is billed for at least 1 CFU while it exists.
- Its output reaches downstream readers at Flink's commit interval, which is the main term in end-to-end freshness (measured in Phase 4).

To record after the first run:

- Whether the `CREATE MATERIALIZED TABLE` statement settles as `RUNNING` or `COMPLETED`, and how its continuous query appears in the statement list.
- Observed delay-to-visible time from the live test (coarse, includes snapshot start-up).
