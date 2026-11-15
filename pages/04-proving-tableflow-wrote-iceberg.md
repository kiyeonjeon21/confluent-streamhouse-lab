---
title: Proving that Tableflow wrote Iceberg
description: A Flink snapshot query returns the same rows whether they come from Kafka or Iceberg. Reading the Tableflow catalog directly with pyiceberg settles it.
date: 2026-11-15
---

*Part 4 of [Building a Streamhouse on Confluent Cloud](index.md).*

## The problem with the obvious test

Tableflow materializes a Kafka topic as an Iceberg table.
On Confluent Cloud, a Flink snapshot query over a Tableflow-enabled topic reads older data from the Iceberg snapshot and the newest records from Kafka, so one query spans history and the present.

The quick start's check is a snapshot query over `flight_history`:

```
flight_id  flights  delayed  impacted
RA214      1        1        64
RA210      1        1        18
RA417      4        1        120
```

I got exactly that, both before and after enabling Tableflow.
Which is the problem: the rows were minutes old and still in Kafka, so the result is identical whether Tableflow works or not.
A matching Flink result proves nothing about Iceberg.

## Read the Iceberg table directly

Tableflow exposes an Iceberg REST catalog per environment:

```
https://tableflow.<region>.aws.confluent.cloud
  /iceberg/catalog/organizations/<org>/environments/<env>
```

It authenticates with OAuth2 client credentials, using a Tableflow API key as `key:secret`, and each cluster's tables sit in a namespace named after the cluster ID.
The documentation's example uses Spark in a container.
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

Setting `oauth2-server-uri` explicitly avoids a deprecation warning: pyiceberg's implicit fallback is slated for removal.

This bypasses Flink and Kafka completely.
About 4.5 minutes after enabling Tableflow, it returned:

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

Six rows, Kafka offsets 0 to 5, matching the source exactly.
The lab's live test now asserts both paths: the Flink aggregate and the rows in the Iceberg snapshot.

## What else showed up

- **A key-less topic works.**
  `flight_history` has no primary key, so Schema Registry has only a value schema.
  The Tableflow overview says both key and value need schemas; Tableflow materialized the topic anyway.
- **The timeline.**
  Terraform created the Tableflow topic in 5 seconds, it reached `RUNNING` about a minute later, the first two snapshots were empty, and the data arrived in the third, about 4.5 minutes after enabling.
  The managed-storage guide warns that first materialization can take about 10 minutes.
- **Metadata columns.**
  The Iceberg schema adds `$$topic`, `$$partition`, `$$offset`, `$$timestamp`, `$$headers`, `$$raw-key`, `$$raw-value`, and a few more.
  `$$offset` makes reconciling Iceberg against Kafka exact.
- **Vended credentials, not remote signing.**
  With Confluent Managed Storage, loading a table returned temporary S3 credentials with an expiry.
  The Spark example turns on S3 remote signing; pyiceberg needed neither.
- **Managed storage has limits.**
  Its tables are reachable only through the Tableflow catalog with vended credentials, and they do not sync to external catalogs such as AWS Glue.
  For Glue or Athena you need your own bucket.

## Still open

I have not proven that Flink's snapshot query read Iceberg rather than Kafka, only that Iceberg holds the right data.
Showing the hybrid read needs records older than the topic's Kafka retention.

Next: [A streaming agent grounded in live state](05-a-streaming-agent-grounded-in-live-state.md).
