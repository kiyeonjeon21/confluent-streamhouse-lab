# Capability 5: Tableflow and Apache Iceberg

## Architecture and purpose

Tableflow materializes a topic as an Iceberg table without a separate pipeline.
With Confluent Managed Storage, Confluent owns the bucket; tables are readable through the Tableflow Iceberg REST Catalog with vended credentials.
A Flink snapshot query on a Tableflow-enabled topic reads older data from the Iceberg snapshot and the newest records from Kafka, so one query spans history and the present.

## Deployment steps

```sh
uv run lab apply history                      # flight_history table and six past flights
# then in terraform/terraform.tfvars:
#   tableflow_topics = ["flight_history"]
scripts/deploy
```

## SQL, configuration, and API examples

- [`sql/05-history`](../../sql/05-history/)
- Terraform: [`terraform/tableflow.tf`](../../terraform/tableflow.tf), resource `confluent_tableflow_topic` with `managed_storage {}` and `table_formats = ["ICEBERG"]`.
- Equivalent CLI: `confluent tableflow topic enable flight_history --cluster <lkc> --storage-type MANAGED`.

## Test procedure

```sh
uv run lab check history
scripts/verify-live capability_5
```

## Expected versus actual results

Expected: `RA214 1 1 64`, `RA210 1 1 18`, `RA417 4 1 120`.
Because the rows are minutes old, the quick start notes the result can come entirely from Kafka; reading from Iceberg needs the topic to age past what Kafka retains, or an external engine.

Actual: **not run yet**.

## Issues, limitations, and lessons learned

Known before running:

- Key and value must both have schemas; `flight_history` has no primary key, so its key is empty. The quick start says this works; confirm.
- No retract tables. Managed storage cannot sync to external catalogs such as Glue.
- Snapshot retention is 10 to 100 snapshots, not configurable.

To record after the first run:

- Time from enabling to the first snapshot.
- A way to prove the Iceberg path is used (for example, reading the table from an external engine through the REST catalog), since the Flink result is identical either way.
