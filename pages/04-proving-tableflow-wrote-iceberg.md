---
title: Proving that Tableflow wrote Iceberg
description: A Flink snapshot query returns the same rows whether they come from Kafka or Iceberg. Reading the Tableflow catalog directly with pyiceberg settles it.
date: 2026-11-15
part: 4
reading_time: 7
sample: 6 rows reconciled by offset
takeaways:
  - "A successful Flink query did not prove Tableflow worked while the same records still existed in Kafka."
  - "A direct pyiceberg read found all six source rows at offsets 0 through 5 in the managed Iceberg catalog."
  - "After Kafka records were deleted, one Flink snapshot query still combined Iceberg-only old rows with Kafka-only new rows."
---

## Why the obvious test proves nothing

Tableflow materializes a Kafka topic as an Iceberg table.
On Confluent Cloud, a Flink snapshot query over a Tableflow-enabled topic reads older data from the Iceberg snapshot and the newest records from Kafka, so one query covers both history and the present.

The quick start checks this with a snapshot query over `flight_history`:

```
flight_id  flights  delayed  impacted
RA214      1        1        64
RA210      1        1        18
RA417      4        1        120
```

I got exactly that, before and after enabling Tableflow.
The rows were minutes old and still in Kafka, though, so the result would have been the same whether Tableflow worked or not, and it told me nothing about Iceberg.

## Evidence 1: read the Iceberg table directly

Tableflow exposes an Iceberg REST catalog for each environment:

```
https://tableflow.<region>.aws.confluent.cloud
  /iceberg/catalog/organizations/<org>/environments/<env>
```

It authenticates with OAuth2 client credentials, using a Tableflow API key as `key:secret`, and each cluster's tables live in a namespace named after the cluster ID.
The documentation's example runs Spark in a container.
pyiceberg is much lighter:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "tableflow",
    uri=endpoint,
    credential=f"{key}:{secret}",
    **{"oauth2-server-uri": f"{endpoint}/v1/oauth/tokens"},
)
table = catalog.load_table((cluster_id, "flight_history"))
rows = table.scan().to_arrow()
```

I set `oauth2-server-uri` explicitly because pyiceberg warns that its implicit fallback is going away.

This reads Iceberg without going through Flink or Kafka.
About 4.5 minutes after I enabled Tableflow, it returned:

```
format v2, 3 snapshots
  07:37:12Z  append
  07:37:13Z  append
  07:40:53Z  append

flight_id  flight_date  delay_minutes  affected_passengers  $$offset
RA417      2026-09-24   12             0                    0
RA417      2026-09-25   0              0                    1
RA417      2026-10-02   95             120                  2
RA210      2026-10-02   40             18                   3
RA417      2026-10-09   5              0                    4
RA214      2026-10-16   130            64                   5
```

That's six rows at Kafka offsets 0 to 5, an exact match with the source.
The lab's live test now checks both: the Flink aggregate and the rows in the Iceberg snapshot.

## What else showed up

`flight_history` has no primary key, so Schema Registry holds only a value schema for it.
The [Tableflow overview](https://docs.confluent.io/cloud/current/topics/tableflow/index.html) says the key and the value both need schemas, but Tableflow materialized this topic without complaint.

Terraform created the Tableflow topic in 5 seconds, and it reached `RUNNING` about a minute later.
The first two snapshots were empty and the data arrived in the third, about 4.5 minutes after I enabled it.
The managed-storage guide warns that the first materialization can take around 10 minutes.

The Iceberg schema adds metadata columns such as `$$topic`, `$$partition`, `$$offset`, `$$timestamp`, `$$headers`, `$$raw-key`, and `$$raw-value`.
With `$$offset`, you can reconcile Iceberg against Kafka row by row.

With Confluent Managed Storage, loading a table returned temporary S3 credentials with an expiry time.
The Spark example in the docs turns on S3 remote signing, which pyiceberg didn't need.

Managed storage tables are reachable through the Tableflow catalog with those vended credentials, and they don't sync to external catalogs such as AWS Glue.
Engines that support the Iceberg REST Catalog and vended credentials can query them directly; Confluent documents Athena for Spark as one example.
If you specifically need Glue catalog integration, you need your own bucket.

## Evidence 2: force a hybrid read

At this point Iceberg held the right data, but nothing showed that Flink had ever read it.
The clean test is to make Kafka forget the data and ask Flink again.

Waiting for retention doesn't work well on Confluent Cloud, because Kafka deletes only closed log segments and the minimum `segment.ms` there is four hours.
The Kafka `DeleteRecords` API gets the same result right away by moving the topic's start offset past every existing record.

On a separate table, `hybrid_flight_history`, I wrote an old batch, waited until Tableflow had it in Iceberg (about 4 minutes), deleted its Kafka records, and wrote a new batch.
At each step a script checked a Kafka consumer reading from the start of the topic, the Iceberg table through pyiceberg, and a Flink snapshot query.

| Step | Kafka | Iceberg | Flink snapshot query |
|---|---|---|---|
| Old batch materialized | OLD-1..4 | OLD-1..4 | OLD-1..4 |
| Kafka records deleted | nothing | OLD-1..4 | OLD-1..4 |
| New batch written | NEW-1, NEW-2 | OLD-1..4 | OLD-1..4, NEW-1, NEW-2 |

```mermaid
accTitle: Tableflow hybrid-read proof
accDescr: Old rows move from Kafka to Iceberg and are deleted from Kafka. New rows remain only in Kafka. A Flink snapshot query reads both stores and returns all six rows.
flowchart LR
  OLD[Old rows<br/>OLD-1..4] --> K1[(Kafka)]
  K1 --> TF[Tableflow]
  TF --> ICE[(Iceberg<br/>OLD-1..4)]
  DEL[DeleteRecords] -. removes old rows .-> K1
  NEW[New rows<br/>NEW-1, NEW-2] --> K1
  ICE --> F[Flink snapshot query]
  K1 --> F
  F --> ALL[All six rows]
```

*At the final step, the two stores contain disjoint data. The combined result is direct evidence that Flink read both paths.*

In the second row Kafka had nothing left and Flink still returned the old rows, so it read them from Iceberg.
In the third row the old rows were only in Iceberg and the new ones only in Kafka, because Tableflow hadn't caught up yet.
One snapshot query returned all six, 6.6 seconds after I wrote the new batch.

> **Proof:** The old rows existed only in Iceberg and the new rows existed only in Kafka. Returning all six in one result rules out either source acting alone.

Deleting the Kafka records left the Iceberg table alone, so a topic can keep a short, cheap retention while queries still see the full history.

My first attempt at this stopped right after the deletion, because my code read the wrong field of the `DeleteRecords` response.
The deletion had gone through, so I checked the topic's watermarks and resumed from that state instead of writing the old batch a second time.
