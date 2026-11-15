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

Actual: **not run yet**.

## Issues, limitations, and lessons learned

To record after the first run:

- Which schema format Flink registers by default for these tables, and whether Lightning and Tableflow accept it unchanged.
- Whether `INSERT ... VALUES` reports `COMPLETED` or passes through `RUNNING` first.
