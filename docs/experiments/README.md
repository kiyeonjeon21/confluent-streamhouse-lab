# Phase 4: evaluation design

This page fixes the methodology before any measurement exists.
No number appears in this repository unless a run in `results/` produced it, and every reported number names its run, date, region, client location, and sample size.

## Ground rules

- **Measure, never estimate.** If a metric cannot be observed directly, say what was observed instead.
- **Report distributions.** p50, p95, p99, and max, with the sample count. A single run is an anecdote.
- **Separate the network.** The client's round-trip time to the region is measured in the same run (TCP connect and TLS handshake timings to the same host) and reported next to every latency.
  From Korea to `us-east-1` the network alone is expected to be large, so either run clients inside the region or subtract nothing and report both.
- **Name the clock.** Freshness compares timestamps from two machines only when both sides use one clock; otherwise use a single client that writes and reads.
- **Warm and cold.** Report the first request after idle separately from steady state, because Basic clusters, compute pools, and Lightning compute scale to zero.
- **Cost per experiment.** Record the billing page delta for the experiment's hours.
- **Raw data stays local.** Every request is appended to `results/<experiment>/<run-id>.jsonl` (gitignored); summaries with methodology go into this folder.

## Experiments

### E1: end-to-end data freshness

Question: from an event written to `flight_status`, how long until `passenger_state` reflects it in a Lightning read?

Method: one client writes an event with a unique `estimated_time` value (the marker) through the Kafka producer API, records the produce acknowledgement time, then polls `queryData` on `bench_passenger_state` until the marker's effect appears.
Writes are spaced by a random wait at least as long as the commit interval, so they do not phase-lock to Flink's commits (see the first runs).
Freshness = first-visible time minus acknowledgement time, both from the client's monotonic clock.
Vary: Flink checkpoint and commit settings if exposed; event rate (idle versus 10 per second background).

Expected shape from documentation: Lightning ingest is under 100 ms, so the total should be dominated by Flink's commit interval.
Measured: dominated by the commit, but Lightning added 3 to 7 s after it (see E2).

### E2: where the freshness goes

Question: of E1's end-to-end time, how much is Flink writing, how much is waiting for Flink's commit, and how much is Lightning?

Method: E1's marker technique, with two Kafka consumers on the materialized table's output topic running on background threads beside the Lightning polling.
A `read_uncommitted` consumer sees Flink's write as soon as it lands; a `read_committed` consumer sees it once Flink's transaction commits.
Each write is therefore observed three times (written, committed, served), and the gaps between them decompose freshness.
The output record's broker timestamp, compared with the input record's, measures Flink's write step on server clocks alone.

### Hybrid read

Question: does a Flink snapshot query on a Tableflow-enabled topic really combine Iceberg history with the Kafka tail?

Method: on a dedicated table, write an old batch, wait until it is in Iceberg, delete its Kafka records with `DeleteRecords`, observe Kafka, Iceberg, and a snapshot query; then write a new batch and observe all three again.
A query that returns old rows absent from Kafka, together with new rows absent from Iceberg, has read both.
Retention cannot stand in for the deletion quickly, because Confluent Cloud enforces a minimum `segment.ms` of four hours.

### E3: Lightning query latency

Question: what is the latency of point lookups, small range scans, and filtered scans?

Method: query shapes fixed in advance (`WHERE passenger_id = ...`, `WHERE risk = 'AT_RISK'`, `LIMIT 200` scan), each issued N times over MCP `queryData`, and over the direct API if Early Access permits.
Datasets: the 3-row quick start data, then a generated dataset of 10,000 and 100,000 passengers with a fixed seed.
Report server-side time if the API returns it, and client time always.

### E4: query throughput and concurrency

Question: at what concurrency does latency degrade or `RATE_LIMIT_EXCEEDED` appear?

Method: closed-loop clients at concurrency 1, 2, 4, 8, 16, 32 for a fixed duration each, recording successes, errors by code, and latency.
Stop at the first sustained error rate above 1%.
Also record the CCU meter for the window, to tie load to cost.

### E5: agent access to real-time context

Question: does the agent see fresh state, how long does a decision take, and how often is the answer correct?

Method: replay the delay event, then run `AI_RUN_AGENT` at fixed offsets after it (for example 5, 15, 30, 60 seconds) and compare the offers with the ground truth computed from the same snapshot.
Record agent latency, tool call count, and correctness of the offer line over at least 20 runs.
Repeat once with an external MCP client (Claude Code) answering the same question.

### E6: failure and recovery

Question: what happens when parts of the pipeline stop?

Scenarios, each with an observation of what readers see and how long recovery takes:

- Stop and resume the materialized table (`stopped = true`) while events keep arriving.
- Evolve the materialized table with `CREATE OR ALTER` to a stricter filter and look for stale keys in Lightning (the documented upsert "zombie" behavior).
- Write a record that breaks the schema contract and observe Tableflow's error handling (`SUSPEND` default) and Lightning's behavior.
- Delete the agent's connection mid-run.

No experiment deliberately exceeds quotas or targets shared infrastructure.

### E7: operational complexity and cost

Question: what does it take to run this, and what does it cost?

Method: a log kept during the lab of every manual step, every surprise, and every console-only operation; the count of Terraform resources and CLI-only steps; and the billing page broken down by product for the lab's lifetime.

## Tooling

Built and unit tested on 2026-11-15 ([`tests/unit/test_bench.py`](../../tests/unit/test_bench.py)).
First runs of E1, E3, and E4: [2026-11-15-first-runs.md](2026-11-15-first-runs.md).
E2 and the hybrid read: [2026-11-15-e2-and-hybrid.md](2026-11-15-e2-and-hybrid.md).

| Piece | Where | What it guarantees |
|---|---|---|
| Isolated pipeline | [`sql/07-bench`](../../sql/07-bench/) | `bench_flight_status`, `bench_passenger_itineraries`, and `bench_passenger_state`, so load never touches the quick start data that capabilities 1 to 6 and the agent read. A unit test keeps the bench materialized table's query identical to `passenger_state` apart from table names. |
| Data generator | [`datagen.py`](../../src/streamhouse_lab/datagen.py) | Same seed and size, same dataset. About 21% of passengers are `AT_RISK`; `expected_risk()` predicts the exact counts the materialized table must converge to. IDs start with `B-`, never colliding with the quick start's `RA` flights. |
| Producer | [`kafka_io.py`](../../src/streamhouse_lab/kafka_io.py) | Serializes with the schema Flink registered (latest version, never auto-registers), `acks=all`, idempotent, and records the broker acknowledgement time on the monotonic clock. |
| Recorder | [`measure.py`](../../src/streamhouse_lab/measure.py) | One JSON-lines file per run under `results/` (gitignored), with a header holding parameters, region, client location, and git revision. |
| Network probe | [`measure.py`](../../src/streamhouse_lab/measure.py) | DNS, TCP connect, and TLS handshake to the MCP host, recorded before and after every run. |
| Topic readers and truncation | [`kafka_io.py`](../../src/streamhouse_lab/kafka_io.py) | Readers per isolation level that deserialize with the registered schemas; `read_all` from the log start; `truncate` with `DeleteRecords`. |
| Runners | [`bench.py`](../../src/streamhouse_lab/bench.py) | E1, E2, E3, E4, and the hybrid read as specified above. E1 and E2 record both Lightning visibility bounds per sample and space writes with seeded jitter; the hybrid runner records what Kafka, Iceberg, and Flink each see at every step and supports `--resume`. |
| Summaries | [`summarize.py`](../../src/streamhouse_lab/summarize.py) | Nearest-rank percentiles, so every reported number was actually observed; errors counted separately. |

Commands:

```sh
uv run lab bench gen --dry-run --passengers 10000 --seed 7        # shape only, writes nothing
uv run lab bench gen --passengers 10000 --seed 7                  # produce the dataset
uv run lab check bench                                            # risk counts in Flink
uv run lab bench e1 --samples 30 --location seoul-laptop
uv run lab bench e2 --samples 30 --location seoul-laptop
uv run lab bench hybrid --location seoul-laptop                   # needs Tableflow on hybrid_flight_history
uv run lab bench e3 --repeat 100 --passengers 10000 --location seoul-laptop
uv run lab bench e4 --levels 1,2,4,8,16,32 --duration 30 --location seoul-laptop
uv run lab bench summarize results/e3/<run-id>.jsonl --metric latency_ms
```

## Running an experiment: approvals

Each step below changes cloud state or bills, so each needs explicit approval.

1. Set `enable_bench_clients = true` and add `bench_passenger_state` to `lightning_topics` in `terraform.tfvars`, then `scripts/deploy`.
   Adds the operator's Kafka and Schema Registry keys and one Lightning Table.
   The Lightning Table needs the topic to exist, so on a fresh deploy apply step 2 first and add the topic afterwards.
2. `uv run lab apply bench`: two tables and a second materialized table, billed at least 1 CFU while it runs.
3. `uv run lab bench gen`: writes the dataset (about 1 MB for 10,000 passengers).
4. For the hybrid read only: `uv run lab apply hybrid`, then add `hybrid_flight_history` to `tableflow_topics` and `scripts/deploy`. Adds one Tableflow topic, billed per hour while enabled.
5. The experiment itself.
6. Afterwards, `scripts/cleanup`, or remove the bench Lightning Table, the Tableflow topic, and stop the bench materialized table.

## Known caveat for the first runs

The client runs in Korea and the lab in `us-east-1`, so every request crosses the Pacific.
The network probe records the round trip so each latency can be read against it.
A client inside `us-east-1` (for example a small EC2 instance or CloudShell) would remove it, at extra cost; it is not part of the current plan.
