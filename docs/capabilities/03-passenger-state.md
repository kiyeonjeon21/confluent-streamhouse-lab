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

Actual (2026-11-15, `scripts/verify-live capability_3`, which replays `ra417-on-time` then `ra417-delayed`): **matches** both before and after.
The test passed in 140 s together with capabilities 1 and 2.

## Issues, limitations, and lessons learned

Observed on the first run:

- **The continuous query is not in the statement list.**
  The `CREATE MATERIALIZED TABLE` statement was `RUNNING` right after submission and `COMPLETED` a few minutes later (`sql_kind = CREATE_MATERIALIZED_TABLE`, bounded).
  The running query is visible only through `GET /sql/v1/.../materialized-tables`, which reports `phase: RUNNING`, a `version`, and `stopped`.
  `uv run lab status` now shows both lists.
- **Flink flags the official quick start query with two CRITICAL warnings:**
  - `UPSERT_AND_PRIMARY_KEYS_DIFFERENT`: the declared primary key `passenger_id` is not the upsert key the query derives, so Flink adds a state-intensive correction operator.
  - `HIGH_STATE_OPERATOR_WITHOUT_TTL`: the regular joins keep state forever without a TTL, which can degrade the statement and raise CFU use.
  Both are harmless at three rows and worth an experiment at scale (Phase 4 E1 to E4), and an evolution with `CREATE OR ALTER` plus a state TTL is a natural capability-3 follow-up.
- **Sink flush is 5 s by default.** The resolved table options include `confluent.sink.buffer-flush.enabled = true` and `confluent.sink.buffer-flush.interval = 5 s`, alongside `kafka.retention.time = 0 ms` on a compacted topic.
  The flush interval is a floor on how fresh downstream readers (Lightning, Tableflow) can be.
- The live test saw the delay reflected within 41 s of the event statement completing.
  That is an upper bound that includes 10 s polling and snapshot-query start-up, not a freshness measurement.
- Billing: the materialized table counts as a running workload for as long as it exists. Stop it (`stopped = true` via the API or Terraform) or tear down between sessions.
