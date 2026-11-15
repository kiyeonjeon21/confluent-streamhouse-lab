---
title: Is a Streamhouse worth it?
description: What a Streamhouse promises, what this lab actually showed for each promise, where it fits, and where it does not.
date: 2026-11-15
---

*Part 7 of [Building a Streamhouse on Confluent Cloud](index.md).*

The first six parts were about how the pieces behave.
This one asks the question a team would actually ask: should we build our next system this way?
Everything below is judged against what the lab measured, not against the marketing.

## The problem a Streamhouse is aimed at

Most companies keep two copies of the business.
Operational systems know what is true right now, and a warehouse or lakehouse knows what happened, minutes or hours later.
Applications, dashboards, and now AI agents each need some of both, so teams build a third and fourth copy in between: a stream processor, a serving database, a cache, a nightly export.
Each copy has its own logic for the same business rule, and they drift.

The Streamhouse answer is to compute the current state once, continuously, from the event stream, and to serve that one result to everyone who needs it, including the history, without another pipeline.

## The usual alternatives

| Approach | How current state is kept | Freshness | History | What you operate |
|---|---|---|---|---|
| Warehouse or lakehouse, batch | Scheduled jobs over loaded data | Minutes to hours | Excellent | Loaders, schedulers, the warehouse |
| Do it yourself streaming | A stream processor writes into a serving database or cache | As fast as you build it | A separate pipeline to the lake | Every component, and the code that keeps them consistent |
| Streaming database | One system maintains views and answers queries | Varies by system | Depends on the system | One system, often outside your event platform |
| Streamhouse on Confluent Cloud | Flink materialized tables over Kafka topics; Lightning Tables serve them; Tableflow writes Iceberg | Measured here: 18 to 68 s | Iceberg, from the same topics | Mostly configuration; the parts are managed |

What sets the last row apart is that the Kafka topic is the contract.
Flink writes a topic, Lightning serves that topic, Tableflow archives that topic, and an agent reads it through MCP.
No layer holds a private copy of another layer's logic.

## The four promises, against the evidence

Confluent's quick start frames the Streamhouse as four solutions.
Here is how each held up.

### 1. Maintain one consistent state

**Held, with a time unit to be aware of.**
`passenger_state` was defined once.
The bounded join computed the same connection times, the Lightning reads and the agent saw the same passengers and risk flags, and a delay flipped two of them to `AT_RISK` without anyone touching the definition ([part 3](03-materialized-tables-in-practice.md)).
The caveat is how that state moves: derived state reached readers in steps about 61 seconds apart, at Flink's commit interval ([part 6](06-how-fresh-is-real-time.md)).
"Consistent" here means everyone agrees; it does not mean everyone is a few milliseconds behind the source.

### 2. Decide across the last five seconds and the last five years

**Half shown.**
The "seconds" half took 18 to 68 seconds, about 42 on average, from a source write to a Lightning read of the derived state.
That is real-time for an operations team and slow for a checkout page.
The "years" half is promising but not finished: Tableflow put the data in Iceberg about 4.5 minutes after it was enabled, and pyiceberg read it directly ([part 4](04-proving-tableflow-wrote-iceberg.md)).
What I did not show is the hybrid read, one Flink snapshot query that takes old data from Iceberg and new data from Kafka, and I did not query the Iceberg tables from an external analytics engine.

### 3. Ground and adapt an AI agent

**Held, and it is the most convincing part.**
A Flink agent on a managed Claude model looked up live flights and hotels through the Real-Time Context Engine and produced exactly the expected rebooking offers on the first run ([part 5](05-a-streaming-agent-grounded-in-live-state.md)).
No model endpoint, no API key in SQL, no retrieval pipeline to build.
Two cautions: the model did not keep to the one-line answer format, so consumers must parse defensively; and on a Basic cluster the agent's credential could administer the cluster, because read-only roles are not available there ([part 2](02-basic-clusters-and-read-only-agents.md)).

### 4. Monitor the business

**Held at small scale, with a warning label.**
The risk flag is a business monitor that updates itself.
But Flink marks the quick start's own query with two CRITICAL warnings: a primary key that does not match the derived upsert key, and joins that keep state forever.
At three passengers that is free; at real volume it is a design review item.

## Where it fits

From what the lab showed, a Streamhouse on Confluent Cloud fits well when:

- The business already runs on Kafka, and the hard part is keeping derived state consistent across many consumers.
- Decisions need current state, but tens of seconds is fresh enough: operations dashboards, exception handling like rebooking, alerting, and agents acting on business state.
- AI agents need live, governed context, and building a retrieval stack is not the goal.
- The team would rather configure managed services than operate a serving database, a cache, and an export pipeline.

## Where it does not, yet

- **Sub-second freshness or read-your-own-write.** With a commit interval near a minute, a user who just changed something will not see it reflected in derived state.
- **Analytical queries on the serving layer.** Lightning returns at most 200 rows per MCP query and rejects aggregates, `GROUP BY`, and joins; that work belongs in Flink or in an engine reading Iceberg.
- **High-rate lookups on large tables, until measured.** On 10,000 rows, each query scanned about 1.4 MB, roughly the whole table, and Lightning bills compute partly by bytes scanned. If that scales with table size, heavy point-lookup traffic could get expensive. This is the first thing I would test before committing.
- **Least privilege on the cheapest cluster.** If agents must hold read-only credentials, budget for a Standard cluster.
- **Clouds other than AWS.** Lightning Tables and the Real-Time Context Engine are AWS only.

## The cost shape

The bill follows what runs, not what exists:

- A materialized table costs at least one Flink CFU for as long as it exists, whether or not data arrives.
- A Tableflow topic costs per hour while enabled.
- Lightning Tables cost storage always, and compute when data arrives or queries scan.
- The agent costs per run.

An idle Basic cluster, compute pool, and Schema Registry cost nothing, which made it practical to build everything in code and tear it down between sessions.

## What I would still want to know

- **Where the fixed 17 seconds of freshness go**, by reading the materialized table's output topic directly and taking Lightning out of the path.
- **Whether the hybrid read works**, with data older than the topic's retention.
- **How scanned bytes and latency scale** at 100,000 rows and beyond, from a client inside the region.
- **What evolution and failure look like**: changing a materialized table in place, stopping it under load, and breaking a schema.
- **The real bill** for each of the above.

## Verdict

The Streamhouse idea holds up: one definition of current state, maintained continuously, served to applications and agents and archived as Iceberg, with the topic as the only contract between layers.
On Confluent Cloud the pieces are real and work with surprisingly little code; the streaming agent in particular went from SQL to a correct answer on the first try.

But "real time" here means tens of seconds, set by Flink's commit interval, and the serving layer is a fast lookup surface, not a query engine.
Pick it for operational decisions and agent context where a minute is fresh enough, measure scan costs on your own data before relying on high-rate lookups, and keep analytical work in Iceberg.

Back to the [overview](index.md).
