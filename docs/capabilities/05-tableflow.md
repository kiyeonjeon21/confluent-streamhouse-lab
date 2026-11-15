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
- Direct Iceberg read, bypassing Flink and Kafka: `uv run lab iceberg flight_history` ([`iceberg.py`](../../src/streamhouse_lab/iceberg.py)).
  It uses pyiceberg against the Tableflow Iceberg REST Catalog at `https://tableflow.<region>.aws.confluent.cloud/iceberg/catalog/organizations/<org>/environments/<env>`, namespace `<cluster id>`, with OAuth2 client credentials from the operator's Tableflow API key.

## Test procedure

```sh
uv run lab check history       # Flink snapshot query: Iceberg snapshot plus Kafka tail
uv run lab iceberg flight_history   # the Iceberg snapshot alone
scripts/verify-live capability_5   # asserts both
```

## Expected versus actual results

Expected: `RA214 1 1 64`, `RA210 1 1 18`, `RA417 4 1 120`.
Because the rows are minutes old, the quick start notes the result can come entirely from Kafka; reading from Iceberg needs the topic to age past what Kafka retains, or an external engine.

Actual (2026-11-15, us-east-1, Basic cluster): **matches**, through both paths.

Flink snapshot query (`uv run lab check history`), before and after Tableflow was enabled:

```
flight_id  flights  delayed  impacted
RA214      1        1        64
RA210      1        1        18
RA417      4        1        120
```

Iceberg snapshot read directly (`uv run lab iceberg flight_history`), 4.5 minutes after enabling:

```
format v2, 3 snapshots
  2026-11-15T07:37:12Z  append
  2026-11-15T07:37:13Z  append
  2026-11-15T07:40:53Z  append

flight_id  flight_date  delay_minutes  affected_passengers  $$offset
RA417      2026-09-24   12             0                    0
RA417      2026-09-25   0              0                    1
RA417      2026-10-02   95             120                  2
RA210      2026-10-02   40             18                   3
RA417      2026-10-09   5              0                    4
RA214      2026-10-16   130            64                   5
```

## Issues, limitations, and lessons learned

Observed on the first run:

- **A key-less topic works.** `flight_history` has no primary key, so Schema Registry holds only `flight_history-value`, no key subject.
  The Tableflow overview says both key and value need schemas; in practice Tableflow materialized the topic without complaint, as the quick start implies.
- **Timeline:** Terraform created the Tableflow topic in 5 s (`PENDING`); it was `RUNNING` about a minute after apply; the first two snapshots (07:37:12 and 07:37:13) were empty; the six rows arrived in a third snapshot at 07:40:53, about 4.5 minutes after enabling.
  The managed-storage quick start warns that first materialization can take around 10 minutes.
- **Defaults chosen by Tableflow** (from `confluent tableflow topic describe`): compaction and partitioning enabled, error handling `SUSPEND`, snapshot retention 7 days.
- **The Iceberg schema adds Tableflow metadata columns**, prefixed `$$` under the default naming scheme: `$$topic`, `$$partition`, `$$headers`, `$$leader-epoch`, `$$offset`, `$$timestamp`, `$$timestamp-type`, `$$raw-key`, `$$raw-value`. `$$offset` makes Kafka-to-Iceberg reconciliation exact.
- **Managed storage vends temporary S3 credentials** with each table load (`s3.access-key-id`, `s3.session-token`, an expiry) rather than using S3 remote signing, which the Spark example in the docs enables. pyiceberg 0.12 reads the table with those credentials and no extra configuration.
- pyiceberg warns unless `oauth2-server-uri` is set; the lab sets it to `<catalog>/v1/oauth/tokens`.
- **Open:** whether Flink's snapshot query actually read Iceberg here. With six minutes-old rows still in Kafka, the quick start says the result can come entirely from Kafka, and the output is identical either way. Proving the hybrid read needs records older than the topic's Kafka retention, which is a Phase 4 item.
- Billing: Tableflow charges a topic-hour for every hour the topic stays enabled, even with no new data.
