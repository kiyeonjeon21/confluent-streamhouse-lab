# Capability 1: Kafka source tables and sample events

## Architecture and purpose

River Air's operational data arrives as three streams: flight status, passenger itineraries, and hotel availability.
In Confluent Cloud, `CREATE TABLE` in Flink creates the Kafka topic and registers its key and value schemas in Schema Registry in one step.
Each table has a primary key, upsert changelog mode, and a compacted topic, so the topic holds the latest value per key.
That shape is what later capabilities need: materialized-table joins, Lightning upsert mode (compacted topic plus key), and Tableflow (schemas on key and value).

## Deployment steps

```sh
uv run lab apply sources
```

Statements run in file order as `lab-sources-<file>` and are skipped on re-runs, so seed events are written once.

## SQL, configuration, and API examples

- DDL: [`sql/01-sources/01-03`](../../sql/01-sources/)
- Seeds: [`sql/01-sources/11-13`](../../sql/01-sources/)
- Each statement is sent to `POST /sql/v1/organizations/{org}/environments/{env}/statements` with `sql.current-catalog` and `sql.current-database` set from Terraform outputs.

## Test procedure

```sh
uv run lab check sources
scripts/verify-live capability_1
```

## Expected versus actual results

Expected: five flights all `ON_TIME`; three passengers, `P-003` with no connecting flight; three hotels, `Airport Bayfront Inn` with 0 rooms.

Actual (2026-11-15, us-east-1, Basic cluster, `uv run lab check sources` and `scripts/verify-live capability_1`): **matches**.

```
flight_id  origin  destination  estimated_time           status
RA210      SFO     SEA          2026-11-04 14:00:00.000  ON_TIME
RA214      SFO     SEA          2026-11-04 15:30:00.000  ON_TIME
RA226      SFO     SEA          2026-11-04 17:45:00.000  ON_TIME
RA232      SFO     SEA          2026-11-05 07:15:00.000  ON_TIME
RA417      ORD     SFO          2026-11-04 13:00:00.000  ON_TIME

hotel                 available_rooms  nightly_rate
Airport Bayfront Inn  0                189.00
Harbor Point Hotel    12               214.00
Runway View Suites    5                239.00

passenger_id  inbound_flight_id  connecting_flight_id
P-001         RA417              RA210
P-002         RA417              RA214
P-003         RA417              NULL
```

## Issues, limitations, and lessons learned

- `uv run lab apply sources` took 23 s wall clock for six statements; every statement ended `COMPLETED` (`CREATE_TABLE` and `INSERT_INTO`, both reported as bounded).
- One snapshot check took roughly 7 to 10 s end to end on this tiny dataset (`lab check sources` ran three in 22 s), so snapshot queries are a cheap but not instant verification tool.
- The materialized table built on these sources reports `key.format` and `value.format` as `avro-registry`, the Flink default; the source tables were created with the same defaults. Lightning and Tableflow both accept Avro.
- Snapshot results come back as strings (`"0"`, `"189.00"`), so tests compare string forms.
