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

Method: one client writes an event with a unique `estimated_time` value (the marker) through the Kafka producer API, records the produce acknowledgement time, then polls `queryData` on `passenger_state` every 100 ms until the marker's effect appears.
Freshness = first-visible time minus acknowledgement time, both from the client's monotonic clock.
Vary: Flink checkpoint and commit settings if exposed; event rate (idle versus 10 per second background).

Expected shape from documentation: Lightning ingest is under 100 ms, so the total should be dominated by Flink's commit interval.

### E2: streaming update latency inside Flink

Question: how long does the materialized table take to emit an updated row, independent of Lightning?

Method: same marker technique, reading the `passenger_state` topic directly with a Kafka consumer (record timestamp and consume time) instead of Lightning.
E1 minus E2 isolates the Lightning serving hop.

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

## Tooling to build before E1

- A seeded data generator for `flight_status` and `passenger_itineraries` (Kafka producer with Schema Registry), so datasets are reproducible.
  The generator needs a Kafka API key and ACLs for a producer service account, which the platform does not create yet.
- A measurement harness that writes JSON lines with monotonic timestamps and the client's network timings.
- A summary script that turns the JSON lines into percentile tables.

Each of these is added, reviewed, and unit tested before any experiment that uses it is approved.
