# First runs: E1, E3, E4 (2026-11-15)

Every number below comes from the raw files named in each section (kept locally under `results/`, gitignored).
Percentiles are nearest-rank, so each is a value that was actually observed.

## Setup

| Item | Value |
|---|---|
| Region | AWS `us-east-1` |
| Cluster | Basic, single zone |
| Flink compute pool | `max_cfu = 5` |
| Dataset | `lab bench gen --passengers 10000 --seed 7`: 1,021 flights, 10,000 passengers |
| Dataset check | `bench_passenger_state` converged to OK 6,387, AT_RISK 2,103, NO_CONNECTION 1,510, exactly as `datagen.expected_risk()` predicted |
| Read path | Real-Time Context Engine over MCP (`queryData`), one long-lived session per client, context-reader Global API key |
| Client | MacBook in Seoul (`--location seoul-laptop`), Python 3.12, `mcp` 2.3.0, `confluent-kafka` 2.16.1 |
| Network to `mcp.us-east-1.aws.confluent.cloud` | TCP connect 190 to 199 ms, TLS handshake 204 to 316 ms, measured before and after each run |
| Code | E1 first run: `103fa83` plus the librdkafka log-level change. E3, E4, E1 second run: `103fa83` plus the changes committed as `3aab2d1` (E1 jitter, which does not affect E3 or E4) |

Every latency below includes one trans-Pacific round trip of about 190 ms.
None of these numbers describe server-side latency on its own.

## E1, first run: phase-locked to the commit interval (methodology finding)

Raw: `results/e1/20261115T080137Z-668ac2.jsonl`.
Command: `lab bench e1 --samples 30 --poll 0.25 --gap 5` (no jitter; the option did not exist yet).

| | n | errors | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| visible by (`seen_by_ms`) | 30 | 0 | 55.9 s | 56.8 s | 59.7 s | 59.7 s |

The first sample became visible between 15.7 s and 17.4 s after the broker acknowledged the write.
The other 29 were all between 52.5 s and 59.7 s.
Their visibility times were 08:02:58, 08:03:59, 08:05:01, and so on: one every 61 to 62 seconds.

What happened: each write was sent 5 s after the previous sample became visible, which is just after a Flink commit, so every write waited almost a full commit interval.
This run is therefore not a freshness distribution.
What it does show:

- `passenger_state`-style output reaches Lightning in discrete steps about 61 s apart, the Flink commit interval, not continuously and not at the 5 s sink flush.
- The worst case, a write that just misses a commit, was about 56 to 60 s from acknowledgement to visibility in this setup.

The runner now spaces writes by a seeded uniform random wait, so write times fall uniformly across the interval (commit `3aab2d1`).

## E1, second run: freshness with writes spread across the commit interval

Raw: `results/e1/20261115T083919Z-7ba038.jsonl`.
Command: `lab bench e1 --samples 30 --poll 0.25 --gap 5 --jitter 60 --seed 7`; the waits between samples ranged from 7.2 s to 63.6 s.

| | n | errors | min | p50 | p95 | p99 | max | mean |
|---|---|---|---|---|---|---|---|---|
| visible by (`seen_by_ms`) | 30 | 0 | 17.8 s | 46.1 s | 66.9 s | 67.6 s | 67.6 s | 42.4 s |
| not visible before (`not_seen_before_ms`) | 30 | 0 | | 44.4 s | 65.1 s | | 65.8 s | |

All 30 visibility times, sorted, in seconds after the broker acknowledgement:
17.8, 19.1, 20.6, 21.1, 23.9, 25.0, 30.0, 30.6, 32.6, 34.3, 36.1, 37.3, 38.8, 42.3, 46.1, 46.6, 47.4, 47.8, 48.6, 50.0, 50.2, 51.4, 52.4, 53.5, 53.7, 57.5, 59.5, 64.6, 66.9, 67.6.

Measurement resolution (the gap between the two bounds) was 1.6 s to 2.9 s, median 1.8 s: one MCP query of about 0.8 s plus the 0.25 s poll interval and its own round trip.

Reading:

- The values spread over about 50 s, from 17.8 s to 67.6 s, as expected when write times are uniform across a commit interval of about 61 s.
- The smallest value in both runs is about 17 to 18 s (17.4 s in the first run, 17.8 s here). A write that lands just before a commit still takes that long to appear.
- So the data fit a simple model: freshness = a fixed part of about 17 s + the wait for the next commit, between 0 and about 61 s.
  This is an interpretation of 30 samples, not a proven decomposition.
- What the fixed 17 s consists of (Flink work after the commit, transactional visibility, Lightning ingest, the read path) is the question for E2, which reads the output topic directly and so takes Lightning out of the measurement.
- For an application, the practical figure on this setup is: a source change is visible in derived Lightning state after 18 to 68 s, about 42 s on average, measured from Seoul.

## E3: Lightning query latency by shape

Raw: `results/e3/20261115T083239Z-e16cc5.jsonl`.
Command: `lab bench e3 --repeat 100 --passengers 10000 --seed 7`, shapes interleaved round-robin over one MCP session.

| shape | rows returned | n | errors | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|
| point lookup by `PASSENGER_ID` | 1 | 100 | 0 | 689 ms | 788 ms | 874 ms | 1,360 ms |
| `RISK = 'AT_RISK' LIMIT 200` | 200 | 100 | 0 | 697 ms | 817 ms | 1,205 ms | 1,261 ms |
| `CONNECTION_MINUTES BETWEEN 45 AND 90 LIMIT 200` | 200 | 100 | 0 | 694 ms | 814 ms | 1,039 ms | 1,222 ms |

- The three shapes are indistinguishable at p50, so at 10,000 rows the cost is dominated by fixed per-call overhead (network, MCP, and engine), not by the query.
- The slowest point lookup (1,360 ms) was the very first call of the run.
- Subtracting one 190 ms round trip leaves roughly 500 ms per call that is not the network path from Seoul; this run cannot split it between the MCP layer and the Lightning engine.

## E4: throughput under concurrency

Raw: `results/e4/20261115T083615Z-48dbe9.jsonl`.
Command: `lab bench e4 --levels 1,2,4,8,16,32 --duration 30`, closed loop, point lookups, one MCP session per worker, all workers in one Python process.

| concurrency | requests in 30 s | throughput | errors | p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| 1 | 40 | 1.3 req/s | 0 | 694 ms | 1,182 ms | 1,305 ms |
| 2 | 86 | 2.9 req/s | 0 | 668 ms | 767 ms | 1,324 ms |
| 4 | 181 | 6.0 req/s | 0 | 636 ms | 702 ms | 1,276 ms |
| 8 | 354 | 11.8 req/s | 0 | 644 ms | 720 ms | 1,277 ms |
| 16 | 698 | 23.3 req/s | 0 | 646 ms | 743 ms | 1,367 ms |
| 32 | 1,219 | 40.6 req/s | 0 | 774 ms | 837 ms | 1,383 ms |

- Throughput grew linearly up to 16 concurrent clients with flat latency.
- At 32, throughput reached 40.6 req/s instead of the roughly 46 req/s linear scaling would give, and p50 rose by about 130 ms.
- No request failed and no `RATE_LIMIT_EXCEEDED` appeared, so the documented rate limit was not reached at 40 req/s.
- The knee at 32 may be the client: 32 TLS sessions on one asyncio loop on a laptop. A run with several client processes, or from inside the region, is needed before attributing it to the service.

## Scanned bytes (console, after the runs)

The Lightning Tables page in the console showed `bench_passenger_state` with 7.96 GB scanned and a size of 0 B; `passenger_state` showed 317.76 KB scanned.
The recorded runs sent 5,830 queries to `bench_passenger_state` (E1 first run 1,643 polls, E1 second run 1,309, E3 300, E4 2,578), about 1.4 MB scanned per query on a 10,000-row table, most of them primary-key lookups.
This suggests queries scan most of the table rather than seeking one key, which would make scanned bytes, and so CCU cost, grow with table size.
Unconfirmed: the reported size of 0 B gives nothing to compare against. Measure at 100,000 rows, ideally with the `dp_scanned_bytes` metric from the Metrics API.

## Cost

Not yet broken down: the billing page lags, so the E7 cost record for this session will be read from the invoice once the day closes.

## What to do next

- E1: separate Flink's commit step from Lightning's ingest by reading the `bench_passenger_state` topic directly with a Kafka consumer (E2).
- E3 and E4: repeat from a client inside `us-east-1` to remove the trans-Pacific round trip, and at 100,000 passengers to see whether row count changes the picture.
- E4: spread workers across processes to rule out the client as the bottleneck at 32.
