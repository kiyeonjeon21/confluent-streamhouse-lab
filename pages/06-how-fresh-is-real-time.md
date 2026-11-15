---
title: How fresh is "real time"?
description: Measured freshness from a Kafka write to a Lightning read, query latency by shape, and concurrency, plus the benchmark bug that doubled the first result.
date: 2026-11-15
---

*Part 6 of [Building a Streamhouse on Confluent Cloud](index.md).*

Lightning Tables are documented to update within 100 ms of data arriving in a topic.
That is the serving layer.
What an application cares about is end to end: a source change, through Flink, to a read of the derived state.
So I measured that.

## Setup

| Item | Value |
|---|---|
| Region | AWS `us-east-1`, Basic cluster, Flink pool with at most 5 CFU |
| Data | A seeded generator: 1,021 flights and 10,000 passengers, written to separate `bench_` tables so the quick start data stays untouched |
| Check | The bench materialized table converged to exactly the risk counts the generator predicted: 6,387 OK, 2,103 AT_RISK, 1,510 NO_CONNECTION |
| Read path | Real-Time Context Engine over MCP, one long-lived session per client |
| Client | A laptop in Seoul |
| Network | 190 to 210 ms TCP connect to the MCP host, measured before and after every run |

Every latency below includes that trans-Pacific round trip.
Percentiles are nearest-rank, so each one is a value that was actually observed.

## Freshness: method

For each sample, the client writes a flight update with a unique `estimated_time` through the Kafka producer (`acks=all`) and records the broker's acknowledgement time.
It then polls `queryData` for the probe passenger's `INBOUND_ARRIVAL` until the new value appears.

Each sample records two bounds:

- **not visible before**: the start of the last poll that did not see the value.
- **visible by**: the end of the first poll that did.

The true visibility time lies between them.
The gap, 1.6 to 2.9 s with a median of 1.8 s, is the resolution: one MCP query takes about 0.8 s from Seoul.

## Freshness: the first run was wrong

The first run of 30 samples reported a median of 55.9 s and a maximum of 59.7 s.
But the first sample alone was 17.4 s, and the other 29 all sat between 52.5 and 59.7 s.

The visibility times gave it away: 08:02:58, 08:03:59, 08:05:01, and so on, one every 61 to 62 seconds.
The runner wrote each new sample 5 seconds after seeing the previous one.
Seeing a change means a Flink commit just happened, so every write landed just after a commit and waited almost a full interval.
The benchmark had phase-locked to the system it was measuring.

That run is not a freshness distribution.
It does show two real things: derived state reaches Lightning in discrete steps about 61 seconds apart, and the worst case, a write that just misses a commit, is about 56 to 60 seconds.

The fix is to spread writes uniformly across the interval: wait the 5 seconds plus a seeded random 0 to 60 seconds between samples.

## Freshness: the result

With jitter, 30 samples, from broker acknowledgement to visible in Lightning:

| min | p50 | p95 | max | mean |
|---|---|---|---|---|
| 17.8 s | 46.1 s | 66.9 s | 67.6 s | 42.4 s |

All 30 values, sorted: 17.8, 19.1, 20.6, 21.1, 23.9, 25.0, 30.0, 30.6, 32.6, 34.3, 36.1, 37.3, 38.8, 42.3, 46.1, 46.6, 47.4, 47.8, 48.6, 50.0, 50.2, 51.4, 52.4, 53.5, 53.7, 57.5, 59.5, 64.6, 66.9, 67.6.

They spread over about 50 seconds, as uniform write times across a 61-second interval should.
The smallest value was about 17 to 18 seconds in both runs.
So the data fit a simple model:

> freshness ≈ about 17 s fixed + the wait for the next commit, between 0 and about 61 s

That is an interpretation of 30 samples, not a proven breakdown.
Splitting the fixed 17 seconds between Flink, transactional visibility, and Lightning is the next experiment: read the materialized table's output topic directly with a Kafka consumer, which takes Lightning out of the path.

**In practice:** on this setup, a source change shows up in derived Lightning state after 18 to 68 seconds, about 42 seconds on average.
Lightning's 100 ms is real, but it is not the number that decides freshness here; Flink's commit interval is.

## Query latency by shape

Three query shapes, 100 rounds each, interleaved over one MCP session:

| shape | rows | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| point lookup | 1 | 689 ms | 788 ms | 874 ms | 1,360 ms |
| equality filter | 200 | 697 ms | 817 ms | 1,205 ms | 1,261 ms |
| range filter | 200 | 694 ms | 814 ms | 1,039 ms | 1,222 ms |

The queries, against the 10,000-row `bench_passenger_state`:

```sql
-- point lookup (a random generated passenger each round)
SELECT * FROM bench_passenger_state WHERE PASSENGER_ID = 'B-P001234'
-- equality filter
SELECT PASSENGER_ID, CONNECTION_MINUTES FROM bench_passenger_state
WHERE RISK = 'AT_RISK' LIMIT 200
-- range filter
SELECT PASSENGER_ID FROM bench_passenger_state
WHERE CONNECTION_MINUTES BETWEEN 45 AND 90 LIMIT 200
```

No errors.
The shapes are indistinguishable at the median, so at 10,000 rows the per-call overhead dominates, not the query.
Taking out one 190 ms round trip leaves about 500 ms per call that this run cannot split between the MCP layer and the Lightning engine.
The slowest point lookup was the first call of the run.

Lightning's SQL is deliberately narrow: filters (`=`, `IN`, `LIKE`, `BETWEEN`, null checks), `ORDER BY`, and `LIMIT`, at most 200 rows over MCP, and no aggregates, `GROUP BY`, or joins.
Joins and aggregation belong in Flink, upstream of the Lightning Table.

## Throughput under concurrency

Closed loop: N workers, each with its own MCP session, each sending the next point lookup as soon as the previous one returns, for 30 seconds per level.

| concurrency | requests | throughput | errors | p50 | p95 |
|---|---|---|---|---|---|
| 1 | 40 | 1.3 req/s | 0 | 694 ms | 1,182 ms |
| 2 | 86 | 2.9 req/s | 0 | 668 ms | 767 ms |
| 4 | 181 | 6.0 req/s | 0 | 636 ms | 702 ms |
| 8 | 354 | 11.8 req/s | 0 | 644 ms | 720 ms |
| 16 | 698 | 23.3 req/s | 0 | 646 ms | 743 ms |
| 32 | 1,219 | 40.6 req/s | 0 | 774 ms | 837 ms |

Throughput grew linearly to 16 clients with flat latency.
At 32 it reached 40.6 req/s instead of the roughly 46 linear scaling predicts, and the median rose by about 130 ms.
No request failed and no rate-limit error appeared.
All 32 workers shared one Python process on a laptop, so the knee may be the client; I would not attribute it to the service without running clients in several processes, or inside the region.

## What I would do differently

- **Measure from inside the region.** From Seoul, the round trip alone is more than a quarter of every query's latency (about 190 of 690 ms).
- **Write the benchmark against the system's clock, not your own.** A fixed delay between samples looked harmless and quietly synchronized the benchmark with Flink's commits.
  Randomized spacing is a one-line fix once you know to look for it.
- **Record bounds, not points.** Reporting both "not visible before" and "visible by" made the measurement resolution explicit and the phase-locking obvious.

That concludes the series.
Back to the [overview](index.md).
