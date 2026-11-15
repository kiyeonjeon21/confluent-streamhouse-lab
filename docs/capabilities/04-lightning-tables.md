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

Actual: **not run yet**.

## Issues, limitations, and lessons learned

Known before running:

- AWS only; value schema required; upsert needs a compacted, keyed topic; no key schema changes.
- At most 200 rows per query; no OFFSET, COUNT, or GROUP BY; 20 s timeout; rate limited.
- The upstream repository saw a frozen duplicate row per key on `flight_status` after enabling (2026-10-28 run); check for it here.

To record after the first run:

- Whether the direct API and `confluent lightning query` work without Early Access enrollment.
- Time from enabling to Active.
