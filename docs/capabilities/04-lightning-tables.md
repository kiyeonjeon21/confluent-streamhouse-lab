# Capability 4: Lightning Tables and low-latency queries

## Architecture and purpose

A Lightning Table serves a topic's current state for low-latency SQL reads.
It picks upsert mode for compacted, keyed topics (last write wins, tombstones delete) and append mode otherwise.
It updates within 100 ms of data arriving in the topic.
Enabling it is an RTCE topic: the same object backs the Real-Time Context Engine (MCP) and the Lightning query API.

## Deployment steps

Add to `terraform/terraform.tfvars` (copy from [the example](../../terraform/terraform.tfvars.example)):

```hcl
lightning_topics = {
  passenger_state = "One current row per River Air passenger: ..."
  flight_status   = "Latest status of every River Air flight: ..."
  hotel_inventory = "Hotels near SFO with available rooms and nightly rate."
}
```

Then `scripts/deploy`.
Each table moves through Pending, Provisioning, and Active over several minutes.
The descriptions are model-readable: agents see them in `listTopics`.

## SQL, configuration, and API examples

- Terraform: [`terraform/lightning.tf`](../../terraform/lightning.tf), resource `confluent_rtce_topic`.
- MCP (GA path):
  ```sh
  uv run lab context topics
  uv run lab context metadata passenger_state
  uv run lab context query passenger_state "SELECT * FROM passenger_state WHERE risk = 'AT_RISK'" --rows 10
  ```
- Direct API (Early Access): `POST https://sql.<region>.aws.confluent.cloud/query/v1` with `{"catalog_name": env, "database_name": cluster, "query": ...}` and a Global API key.
  Small fast results return inline (200); larger ones return 202 and a job to poll.
- CLI: `confluent lightning query --sql "..." --cluster <lkc> --environment <env> -o json`, using your own login.

## Test procedure

```sh
scripts/verify-live capability_4
```

## Expected versus actual results

Expected: `listTopics` shows the three topics with their descriptions; `queryData` on `passenger_state` returns `P-001` and `P-002` as `AT_RISK`.

Actual (2026-11-15, us-east-1, Basic cluster, `uv run lab context ...` and `scripts/verify-live capability_4`): **matches**, after one permission change described below.

```
listTopics   flight_status, hotel_inventory, passenger_state, all "online", with the configured descriptions
queryData    SELECT * FROM passenger_state WHERE risk = 'AT_RISK'
             ['P-001', 'RA417', 'RA210', 'SEA', '2026-11-04 14:50:00', '-50', 'AT_RISK']
             ['P-002', 'RA417', 'RA214', 'SEA', '2026-11-04 14:50:00', '40', 'AT_RISK']
queryData    SELECT FLIGHT_ID, STATUS, ESTIMATED_TIME FROM flight_status
             five rows, one per flight, RA417 DELAYED
```

## Issues, limitations, and lessons learned

Observed on the first run:

- **On a Basic cluster, the read-only context reader is impossible.**
  Terraform's `DeveloperRead` binding failed at topic scope and at cluster scope with `403 Forbidden - Basic Clusters can not use resource roles`.
  A Kafka `READ` ACL on all topics was accepted by Kafka, but the Real-Time Context Engine ignored it: `listTopics`, `getMetadata`, and `queryData` all returned `FE_AUTHORIZATION_FAILED` ("Access denied by authorization service"), still after more than 10 minutes.
  `CloudClusterAdmin` on the lab cluster works immediately; it is what [`terraform/lightning.tf`](../../terraform/lightning.tf) now grants on Basic, and it means a leaked reader key could administer this cluster.
  On Standard the configuration keeps the documented `DeveloperRead` bindings.
- Enabling three Lightning Tables took 2 min 9 s in Terraform (`confluent_rtce_topic`), and all three were `online` when first queried.
- `getMetadata` reports column names upper-cased (`PASSENGER_ID`) and timestamps as `TIMESTAMP(6)`, while Flink declares `TIMESTAMP(3)`. Lightning SQL accepted lower-case names in `WHERE risk = 'AT_RISK'`.
- No duplicate rows per key appeared on `flight_status`, unlike the upstream report, although RA417 had been written five times before enabling.
- `confluent lightning query` (user login) worked without any Early Access enrollment and printed the same two rows in 2.5 s wall clock.
- A single MCP `queryData` took 0.7 to 1.0 s from a client in Korea to `us-east-1`; that includes the trans-Pacific round trip and is an anecdote, not a measurement.
- **Propagation into Lightning is far slower than Lightning's own 100 ms ingest.**
  Replaying an event and polling `queryData` once per second (2026-11-15, n = 2): the event's `INSERT` statement took 10.3 to 10.4 s to complete, and `passenger_state` in Lightning changed 34.6 s and 51.4 s after that.
  Both values fall inside a one-minute window, which matches a Flink commit interval of about one minute (Lightning reads committed records) rather than the 5 s sink flush. Phase 4 experiment E1 measures this properly.
