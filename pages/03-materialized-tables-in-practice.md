---
title: Materialized tables in practice
description: The continuous query that is not in the statement list, two CRITICAL warnings on the official example, and what happened when I stopped it, changed it, and fed it a record it couldn't read.
date: 2026-11-15
part: 3
lang: en
alternate_url: /ko/03-materialized-tables-in-practice.html
translation_key: chapter-03
reading_time: 11
sample: 2 warnings, 3 failure tests at 100,000 rows
takeaways:
  - label: "Observed"
    text: "Stopped and resumed, the table lost nothing. Changed with CREATE OR ALTER, it kept rows the new query drops and stopped updating the rows it already had. One unreadable record failed it, and Lightning kept serving its last state."
  - label: "Interpretation"
    text: "The CREATE statement shows COMPLETED while its continuous query keeps running and billing, and every failure here left readers on stale data with no sign of it."
  - label: "Limitation"
    text: "One run of each failure, on generated data. The CRITICAL warnings had no visible effect at lab size, and I didn't measure how state grows over time."
---

A Flink materialized table on Confluent Cloud is a single object that owns a table definition, its backing compacted topic, its schema, and the continuous query that fills it.
River Air's `passenger_state` joins every itinerary to its inbound and onward flights and keeps one row per passenger with a risk flag.

```sql
CREATE MATERIALIZED TABLE passenger_state (
  PRIMARY KEY (passenger_id) NOT ENFORCED
)
DISTRIBUTED BY (passenger_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert')
AS SELECT
  p.passenger_id,
  ...
  CASE
    WHEN p.connecting_flight_id IS NULL THEN 'NO_CONNECTION'
    WHEN TIMESTAMPDIFF(MINUTE, inbound.estimated_time,
                       onward.estimated_time) < 45 THEN 'AT_RISK'
    ELSE 'OK'
  END AS risk
FROM passenger_itineraries p
JOIN flight_status inbound ON p.inbound_flight_id = inbound.flight_id
LEFT JOIN flight_status onward ON p.connecting_flight_id = onward.flight_id;
```

It did what the docs promise.
When RA417 was delayed, `P-001` and `P-002` flipped to `AT_RISK` and nobody touched the definition.
Three other things took me longer to work out.

## 1. The running query is not in the statement list

Right after I submitted it, the `CREATE MATERIALIZED TABLE` statement showed as `RUNNING`.
A few minutes later it showed as `COMPLETED`, as a bounded statement.
Nothing in the statement list was running any more, and the table kept updating anyway.

The console splits it the same way.
On the Statements tab, both materialized tables in the lab (the quick start's and a load-test copy) show up only as completed `CREATE MATERIALIZED TABLE` statements:

[![Flink Statements tab: both CREATE MATERIALIZED TABLE statements are Completed](assets/flink-statements-mt-completed.png)](assets/flink-statements-mt-completed.png)

The Materialized tables tab next to it shows both running, each holding one CFU:

[![Flink Materialized tables tab: both tables Running at 1 CFU](assets/flink-mt-running.png)](assets/flink-mt-running.png)

*Confluent Cloud console, 2026-11-15. The cluster ID column is hidden. Click any screenshot for full size.*

In the API, the continuous query is under `GET /sql/v1/.../materialized-tables`, which reports its own phase, version, and a `stopped` flag:

```
materialized tables (continuous queries; billed while RUNNING):
  RUNNING    passenger_state v1
```

Stream Lineage does show them, as Flink nodes between the source topics and their output ([part 1](01-the-lab.md) has the screenshot).

If you monitor or clean up Flink by listing statements, you will miss the part that is billed around the clock.
The lab's status command now reads both lists.

## 2. The official example carries two CRITICAL warnings

The materialized-table API also returns warnings.
For the quick start's query, copied exactly from the documentation, it returned two, both `CRITICAL`:

- `UPSERT_AND_PRIMARY_KEYS_DIFFERENT`: the declared primary key, `passenger_id`, is not the upsert key Flink derives from the query, so Flink adds a state-intensive correction step.
- `HIGH_STATE_OPERATOR_WITHOUT_TTL`: the regular joins keep state forever because no time-to-live is set, which can degrade the statement and raise CFU use.

The console shows both at the top of the table's page:

[![Materialized table details: two CRITICAL warnings above the activity tabs](assets/flink-mt-warnings.png)](assets/flink-mt-warnings.png)

With three passengers the warnings had no visible effect, although the materialized table still held one billed CFU.
At production scale I'd address them separately: redesign the query or key so the derived upsert key matches the declared primary key, and add a state TTL to bound the joins.
Changing a materialized table to do that has costs of its own, which section 4 measures.

## 3. The settings that decide freshness

The resolved table options include:

```
'confluent.sink.buffer-flush.enabled'  = 'true'
'confluent.sink.buffer-flush.interval' = '5 s'
'kafka.cleanup-policy'                 = 'compact'
'key.format'                           = 'avro-registry'
'value.format'                         = 'avro-registry'
```

With a 5-second flush I expected downstream readers to see changes within a few seconds.
In my measurements, derived state reached Lightning Tables in steps about a minute apart.
Flink commits its output transaction about once a minute, as Confluent's [delivery guarantees page](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html) documents for the default exactly-once mode, and readers that only see committed data, Lightning included, wait for that commit.
[Part 6](06-how-fresh-is-real-time.md) breaks the time down.

## 4. Stopping it, changing it, and feeding it a bad record

I tried three things on the load-test copy of this table, `bench_passenger_state`, with 100,000 generated passengers, while watching what Flink reported and what Lightning served.

### Stop and resume

Stopping took under two seconds, and the table showed `STOPPED`.
For the next two and a half minutes I kept changing a flight.
Lightning served each change on the source topic right away, and kept serving the old derived row, with both Lightning Tables still `ACTIVE`.

Resuming took eight seconds to `RUNNING`, and the last change reached Lightning 73 seconds after that, a little over one commit interval.
Nothing was lost: the risk counts matched what the generator predicted, before the stop and after the resume.

### Change the query with CREATE OR ALTER

I added one filter, `WHERE p.connecting_flight_id IS NOT NULL`, which drops the 15,024 passengers who don't connect.

The first attempt failed: "Changing distribution of materialized table is not supported."
I had repeated the table's original `DISTRIBUTED BY` clause word for word; leaving it out worked, and version 2 was running 15 seconds later.

Two things then happened, and the [documentation](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html) describes both, as a stricter filter leaving "zombie" rows and as discarding all processing state:

- All 15,024 dropped passengers stayed in the output topic and in Lightning. In upsert mode the new query sends no delete for keys it no longer produces.
- Passengers the table already held stopped updating. I delayed a flight after the change; Lightning served the new flight time, and six minutes later the passengers on it still showed the old arrival. The new version starts with no join state and reads the sources from where the old one stopped, so it never saw those itineraries and has nothing to join the flight change with. A passenger whose flights and itinerary were all written after the change showed up in 14 seconds.

So a query change on a running table is closer to starting a new table that only knows about new data.
Rebuilding the existing keys would mean reprocessing the sources from the beginning, which I didn't test.

### A record nobody can read

I wrote one record to `bench_flight_status` with a valid key and a value that wasn't Avro, as a producer with the wrong serializer would.
The three readers of that topic each did something different:

| Reader | What happened |
|---|---|
| The materialized table | `FAILED` within 30 seconds, naming the topic, partition, and offset |
| Lightning, on the source topic | Skipped the record, stayed `ACTIVE`, and served a valid write a minute later |
| Lightning, on the derived table | Stayed `ACTIVE`, and stale from then on |
| Tableflow | `FAILED` and suspended about two minutes later: "Contact support if you need help resolving this." |

Recovering the materialized table took more than the documented setting.
`ALTER TABLE bench_flight_status SET ('error-handling.mode' = 'log')`, which should send unreadable records to a dead letter table, followed by a stop and a resume, left it failing on the same offset.
Another `CREATE OR ALTER` brought it back, with the same loss of state as above, and the dead letter table was still empty when I checked a minute later.
Tableflow recovered on its own once I set its error handling to `SKIP`.

In all three cases the people reading the data would have seen nothing wrong: Lightning kept answering, from state that had stopped moving.
I'd alert on the materialized table's phase and on the age of the newest row, not on Lightning's status.

## Smaller notes

- A bounded copy of the same join, run as a snapshot query, finished in about 10 seconds and raised no state warnings.
  For exploring, I'd use snapshot queries and only make a materialized table when the result has to stay current.
- A materialized table is billed for at least one CFU for as long as it exists.
  You can stop it through the API or with Terraform's `stopped = true` without dropping it; SQL has `ALTER MATERIALIZED TABLE ... SUSPEND` but no way back.
