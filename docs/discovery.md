# Phase 1: discovery

Findings from 2026-11-15, before any lab resource was created.
Every claim cites where it was checked, so it can be re-verified when Confluent ships changes.

## Local environment

| Tool | Version | How it is used |
|---|---|---|
| Confluent CLI | 4.82.1 | RTCE and Lightning commands, the agent's MCP connection, cleanup checks |
| Terraform | 1.17.2 | All infrastructure |
| Confluent Terraform provider | 2.95.0 (latest, published 2026-11-12) | Pinned `~> 2.95` in `terraform/versions.tf` |
| uv | 0.13.8 | Python tooling and tests |
| GitHub CLI | 2.104.0 | Repository hosting |
| Git | 2.56.1 (Homebrew build first on PATH) | |
| shellcheck, jq | present | Script linting, cleanup verification |

Authentication was checked without printing credentials.
The Confluent CLI is logged in with a user account (credential type: username).
The GitHub CLI is logged in through the system keyring with SSH as the git protocol.
`uv run lab doctor` repeats these checks.

## Organization state before the lab

The organization already holds one environment, `default`, in AWS `ap-northeast-2` with:

- a Basic cluster `cluster_0` with six topics from earlier experiments,
- two Flink compute pools, one in `ap-northeast-2` and the default one in `ap-northeast-1`,
- Schema Registry with six schemas,
- seven API keys from earlier experiments.

The billing page showed $0.65 for April 2026, $0.39 for June 2026, and $0.00 for every other month through November 2026.
Idle Basic clusters, compute pools, and Schema Registry therefore cost nothing; only running work is billed.

The lab does not touch that environment.
It creates its own environment, so `scripts/cleanup` can delete everything the lab made and nothing else.
Retiring the old environment and keys is a separate decision for the owner.

## Feature availability

Source: docs.confluent.io/cloud/current/clusters/regions.html, AWS tables.

| Feature | us-east-1 | ap-northeast-2 (Seoul) |
|---|---|---|
| Basic and Standard clusters | Yes | Yes |
| Flink | Yes | Yes |
| Managed model inference | Yes | Yes |
| Tableflow | Yes | Yes |
| Lightning Tables | Yes | Yes |
| Real-Time Context Engine | Yes | Yes |

`confluent rtce region list` agrees: both regions are listed.
The lab defaults to `us-east-1`, as requested and as the quick start uses.
Seoul is a valid alternative that matters for latency experiments (see [experiments](experiments/README.md)).

Lifecycle labels found in the documentation and provider:

| Capability | Status | Source |
|---|---|---|
| Flink statements API `sql/v1` | GA | flink/operate-and-deploy/flink-rest-api.html |
| `confluent_flink_materialized_table`, `confluent_rtce_topic`, `confluent_tableflow_topic`, `confluent_flink_connection` | GA | Terraform provider 2.95.0 docs |
| Lightning query API used directly, without an agent | Early Access | billing/dimensions/rtce.html |
| Real-Time Context Engine over MCP | no label on the pages read | ai/real-time-context-engine/overview.html |

## How the pieces map to APIs

These were verified in documentation or CLI help, not assumed:

- Enabling a Lightning Table on a topic creates an RTCE topic (`rtce/v1/rtce-topics`).
  The Console, `confluent rtce rtce-topic create`, and Terraform `confluent_rtce_topic` are three front ends for the same object.
- The MCP endpoint is `https://mcp.<region>.aws.confluent.cloud/mcp/v1/context-engine/organizations/<org>/environments/<env>/kafka-clusters/<lkc>`, with HTTP Basic auth from a Global API key.
  It exposes `listTopics`, `getMetadata`, and `queryData`.
- The Lightning query API is `https://sql.<region>.aws.confluent.cloud/query/v1`.
  The upstream quickstart repository still calls `query/v1alpha1`.
- Terraform 2.95.0 can create Global API keys.
  The upstream repository says the provider cannot and uses the CLI; that comment is out of date.
- Terraform `confluent_flink_connection` does not list `MCP_SERVER` as a type, so the agent's connection is created with `confluent flink connection create`, as the quick start does.
- Snapshot queries are statements with the property `sql.snapshot.mode = now`.

## Upstream quickstart repository

`confluentinc/quickstart-streamhouse` (Apache-2.0, reviewed at commit `fcb22b0`, 2026-11-13) holds two labs: Agentic Airline Operations (River Air) and F1 Agentic Pit Decisions.
It deploys with `uv run deploy`, a large Python orchestrator around two Terraform roots, and adds a data generator, a web app, and S3 plus Glue for Tableflow.

What this lab takes from it:

- Flink objects as one-statement SQL files that are also paste-ready in the SQL workspace.
- A dedicated read-only service account with a Global API key for the Real-Time Context Engine.
- The technical boundaries it documents: joins and aggregations belong in Flink, not Lightning; Lightning returns at most 200 rows and rejects OFFSET, COUNT, and GROUP BY; Flink output reaches Lightning at its commit interval.

What this lab does differently, and why:

| Upstream | Here | Reason |
|---|---|---|
| Standard cluster | Basic by default | Every feature this lab uses runs on Basic, and Basic's first eCKU is free |
| Stream Governance Advanced | Essentials | Nothing here needs Advanced |
| Global key created with the CLI | Global key created by Terraform | Provider 2.95.0 supports it; one tool owns the key's lifecycle |
| Tableflow to customer S3 with Glue | Confluent Managed Storage | No AWS account or IAM needed; the trade-off is no external catalog sync |
| One orchestrator script | Terraform for infrastructure, `uv run lab` for SQL, tests for verification | Each step is small enough to review before it costs money |

## Limitations found before building

From the Lightning Tables, Tableflow, Flink, and RTCE documentation:

- Lightning Tables and the Real-Time Context Engine are AWS only.
- A Lightning Table needs a registered Avro, Protobuf, or JSON Schema value schema.
  Upsert mode also needs a compacted topic and a record key; key schema changes are not supported.
- MCP `queryData` returns at most 200 rows, times out after 20 seconds, requires `max_result_rows`, and is rate limited.
- Tableflow is documented to need schemas on both key and value (a key-less topic worked in practice, see [capability 5](capabilities/05-tableflow.md)), does not support retract tables, keeps 10 to 100 snapshots, and, with managed storage, cannot sync to external catalogs.
- Flink reads Tableflow tables only in snapshot queries.
- A materialized table evolution discards state and rebuilds; upsert outputs can keep stale keys that a stricter new query no longer emits.
- A compute pool's `max_cfu` can be raised but never lowered.
- Confluent Cloud enforces a minimum `segment.ms` of four hours (docs.confluent.io/cloud/current/topics/manage.html), so short retention does not delete data quickly; Kafka deletes only closed segments. `DeleteRecords` works and is how the hybrid-read experiment emptied a topic.
- Each user account can have only two Global API keys.
  The lab avoids spending them by giving the key to a service account.
- Basic clusters do not support RBAC roles on resources inside the cluster.
  Confirmed on 2026-11-15: a read-only context reader is impossible on Basic, and the reader needs `CloudClusterAdmin` (see [capability 4](capabilities/04-lightning-tables.md)).

## Found later by measurement

Not in the documentation, or contrary to it; details in [docs/experiments](experiments/README.md):

- A materialized table's continuous query is not in the statement list; it appears only under the materialized-tables API.
- The quick start's `passenger_state` query carries two CRITICAL Flink warnings.
- Derived state reaches readers when Flink commits, about once a minute; Lightning then adds 3 to 7 s, not 100 ms.
- On 10,000 rows, Lightning queries scanned about 1.4 MB each, roughly the whole table, even for key lookups (unconfirmed).
- A Basic cluster cannot give a service account the read-only access the Real-Time Context Engine needs.

## Costs

List prices from confluent.io/confluent-cloud/pricing and the billing documentation, read on 2026-11-15.
Regional multipliers apply, and the Tableflow and model inference rates load dynamically and were not captured, so check the pricing page before each approval.

| Resource | Meter | List price | When it is billed |
|---|---|---|---|
| Basic cluster | eCKU-hour | first eCKU free, then $0.14 | throughput above one eCKU |
| Standard cluster (not used) | eCKU-hour | $0.75, minimum one | every hour it exists |
| Kafka data | GB in, out, stored | $0.05 per GB, $0.08 per GB-month | |
| Flink | CFU-hour, metered per minute | regional; the June 2026 invoice billed $0.2562 in ap-northeast-2 | while any statement runs, at least 1 CFU-minute per statement |
| Lightning Tables and RTCE | CCU-hour and GB-month | $0.105 per CCU-hour, $0.029 per GB-month | while data streams in or queries run; storage always |
| Tableflow | topic-hour and GB processed | see pricing page | every hour a topic is enabled |
| Confluent Managed Storage | GB stored, requests | see pricing page | |
| Managed model inference | per use | see pricing page | each agent run |

The materialized table is a continuous statement, so it consumes at least one CFU for as long as it exists.
At roughly $0.26 per CFU-hour that is about $6 per day if the lab is left up.
Tear down between sessions; everything is reproducible from this repository.
