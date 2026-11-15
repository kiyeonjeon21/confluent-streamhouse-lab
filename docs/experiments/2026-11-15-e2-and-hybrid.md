# E2 and the hybrid read (2026-11-15, second session)

A fresh deployment of the lab in AWS `us-east-1` on a Basic cluster, with only what these two experiments need: the `bench_` pipeline with 10,000 generated passengers (seed 7), a Lightning Table on `bench_passenger_state`, and Tableflow on `hybrid_flight_history`.
Raw files are under `results/` (gitignored).

## Hybrid read: proven

Question: does one Flink snapshot query read older data from Tableflow's Iceberg table and newer data from Kafka?

Method (`lab bench hybrid`, [`bench.run_hybrid`](../../src/streamhouse_lab/bench.py)):

1. Write an old batch (`OLD-1` to `OLD-4`) to `hybrid_flight_history` and wait until Tableflow has it in Iceberg.
2. Delete every record from the Kafka topic with the Kafka `DeleteRecords` API, as retention eventually would.
   Retention itself is not practical here: Confluent Cloud enforces a minimum `segment.ms` of four hours, and Kafka deletes only closed segments.
3. Observe Kafka (a `read_committed` consumer from the log start), Iceberg (pyiceberg through the Tableflow REST catalog), and a Flink snapshot query.
4. Write a new batch (`NEW-1`, `NEW-2`) and observe all three again.

Raw: `results/hybrid/20261115T101013Z-73e2b7.jsonl` (steps 1 and 2), `results/hybrid/20261115T101545Z-0bd912.jsonl` (steps 3 and 4).
The first run stopped with an exception after the deletion had succeeded: the code read `offset` from the `DeleteRecords` result, whose field is `low_watermark`.
The fix (commit after `63ebc13`) added `--resume`, and the second run started by observing the truncated state rather than writing the old batch again.
Before resuming, the topic's watermarks were checked directly: partition 5 at low 5, high 5 (four records and a transaction marker), all other partitions at 0, 0.

| Time (UTC) | Step | Kafka | Iceberg | Flink snapshot query |
|---|---|---|---|---|
| 10:14:50 | Old batch in Iceberg, 240.6 s after writing it | OLD-1..4 | OLD-1..4 | OLD-1..4 |
| 10:16:07 | After deleting the Kafka records | (none) | OLD-1..4 | OLD-1..4 |
| 10:16:49 | New batch written; Flink showed it 6.6 s later | NEW-1, NEW-2 | OLD-1..4 | OLD-1..4, NEW-1, NEW-2 |

Reading:

- After the deletion, Kafka held nothing, yet the snapshot query returned the old batch, so it read Iceberg.
- After the new batch, the old rows were only in Iceberg and the new rows were only in Kafka (Tableflow had not materialized them yet), and one query returned all six.
  That is the hybrid read: Iceberg history plus the Kafka tail, in a single statement.
- Deleting Kafka records did not remove the rows from the Iceberg table.

Verdict recorded by the runner: `hybrid` (old only in Iceberg: true; Flink read Iceberg after truncation: true; Flink read Iceberg and Kafka together: true).

## E2: where the freshness goes

Question: E1 measured 18 to 68 s from a write to a Lightning read of the derived state, and the data fit "about 17 s fixed plus the wait for a commit about once a minute". Where does that time go?

Method (`lab bench e2`, [`bench.run_stage_latency`](../../src/streamhouse_lab/bench.py)): exactly E1's method (unique marker write, Lightning polled every 0.25 s plus query time, writes spaced by 5 s plus a seeded uniform 0 to 60 s), with two Kafka consumers reading `bench_passenger_state` on background threads:

- `read_uncommitted` sees a record as soon as Flink writes it to the output topic.
- `read_committed` sees it only after Flink's transaction commits.

The difference between them is the commit wait; the difference between `read_committed` and Lightning is the serving hop.
Each consumer also recorded the output record's broker timestamp, which, compared with the input record's broker timestamp from the producer acknowledgement, measures the Flink write step on server clocks only.

Raw: `results/e2/20261115T101659Z-d2f0ba.jsonl`. Code: `63ebc13` plus the uncommitted `DeleteRecords` fix (which E2 does not use). Waits between samples ranged from 7.2 s to 63.6 s. Network to the MCP host: TCP connect 194 ms before, 260 ms after.

Results, 30 samples, 0 errors, seconds after the broker acknowledged the write:

| Stage | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|
| Flink wrote the output (`read_uncommitted`) | 0.2 | 2.6 | 4.8 | 5.0 | 2.7 |
| Output committed (`read_committed`) | 11.3 | 38.5 | 61.1 | 61.8 | 36.7 |
| Visible in Lightning (visible by) | 17.5 | 44.3 | 66.9 | 68.2 | 42.8 |
| Visible in Lightning (not visible before) | 15.6 | 42.6 | 65.2 | 66.3 | 41.1 |

Derived, per sample:

| Interval | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|
| Commit wait (committed minus uncommitted) | 7.1 | 36.9 | 57.3 | 57.5 | 34.1 |
| Lightning after commit, lower bound | 3.2 | 4.4 | 4.7 | 5.3 | 4.3 |
| Lightning after commit, upper bound | 5.0 | 6.1 | 6.5 | 7.1 | 6.0 |
| Flink write on server clocks (output minus input record timestamp) | 0.3 | 2.5 | 4.7 | 4.9 | 2.6 |

All committed times, sorted: 11.3, 12.1, 15.7, 16.6, 18.8, 22.0, 23.6, 24.0, 26.6, 28.0, 30.1, 31.8, 32.1, 36.7, 38.5, 40.8, 41.1, 42.7, 43.2, 44.5, 45.3, 46.4, 47.0, 47.3, 47.5, 52.1, 53.7, 59.5, 61.1, 61.8.

Reading:

- **Flink's write step is the 5 s sink flush.** Output reached the topic 0.2 to 5.0 s after the input, averaging 2.7 s, as expected for writes landing uniformly within a 5 s flush interval.
  The server-clock measurement agrees with the client one (p50 2.5 s against 2.6 s), which also validates the client-side method.
- **The commit wait is most of the freshness.** Committed visibility spread from 11 to 62 s, consistent with Flink committing its Kafka transaction about once a minute.
  Even a write that just made a commit waited at least 7 s between being written and being committed.
- **Lightning lags the commit by about 4 to 6 s.** The lower bounds (3.2 to 5.3 s) say that a query started that long after the committed record reached a consumer in Seoul still did not see it.
  The documentation says Lightning updates within 100 ms of data arriving in the topic; for this transactional Flink output, measured from commit visibility, it was 3 to 7 s.
  Because the consumer is in Seoul, it saw the commit somewhat after the broker did, so the server-side lag is, if anything, slightly longer.
- **Commits run on a fixed clock.** Consecutive samples' commits, as the `read_committed` consumer saw them, were 59.7 to 62.0 s apart, or about 122 s when an interval passed with no sample.
  A least-squares fit of evenly spaced commits gives one every 61.18 s, with every observation within 0.97 s of it over 34 intervals (`uv run lab chart commit-clock results/e2/20261115T101659Z-d2f0ba.jsonl`).
  When a write arrives decides only how long it waits for the next commit.
- **E1's "fixed 17 s" decomposes** into the smallest commit visibility (about 11 s: flush plus checkpoint plus transaction commit) plus the Lightning hop (about 5 s).
- Total freshness matched E1 closely: p50 44.3 s here against 46.1 s in E1, mean 42.8 s against 42.4 s.
