---
title: Is a Streamhouse worth it?
description: What a Streamhouse promises, what this lab actually showed for each promise, where it fits, and where it does not.
date: 2026-11-15
part: 7
lang: en
alternate_url: /ko/07-is-a-streamhouse-worth-it.html
translation_key: chapter-07
reading_time: 8
sample: Evidence from all 6 capabilities
takeaways:
  - label: "Observed"
    text: "One Flink definition of passenger state fed both the Lightning reads and the agent, while Tableflow preserved history without another export pipeline."
  - label: "Interpretation"
    text: "As tested, with default exactly-once delivery, it fits operational decisions that can wait tens of seconds. It doesn't fit sub-second read-your-write paths or analytics on the serving layer."
  - label: "Limitation"
    text: "Before production I'd test scan cost at realistic table sizes and use a cluster type that gives agent credentials least privilege."
---

The first six parts were about how the pieces behave.
This one is about the question a team would ask: should we build our next system this way?
I'm judging it only by what the lab measured.

## The decision frame

Most companies keep two copies of the business.
Operational systems know what's true right now, and a warehouse or lakehouse knows what happened, minutes or hours later.
Applications, dashboards, and now AI agents each need a bit of both, so teams build more copies in between: a stream processor, a serving database, a cache, a nightly export.
Each copy implements the same business rule in its own code, and over time they drift apart.

A Streamhouse computes the current state once, continuously, from the event stream, and serves that one result, history included, to everyone who needs it without another pipeline.

## The usual alternatives

| Approach | How current state is kept | Freshness | History | What you operate |
|---|---|---|---|---|
| Warehouse or lakehouse, batch | Scheduled jobs over loaded data | Minutes to hours | Excellent | Loaders, schedulers, the warehouse |
| Do it yourself streaming | A stream processor writes into a serving database or cache | As fast as you build it | A separate pipeline to the lake | Every component, and the code that keeps them consistent |
| Streaming database | One system maintains views and answers queries | Varies by system | Depends on the system | One system, often outside your event platform |
| Streamhouse on Confluent Cloud | Flink materialized tables over Kafka topics; Lightning Tables serve them; Tableflow writes Iceberg | Measured here: 18 to 68 s | Iceberg, from the same topics | Mostly configuration; the parts are managed |

In the last row, every layer works off the same Kafka topics.
Flink writes a topic, Lightning serves it, Tableflow archives it, and an agent reads it through MCP, so the business logic lives in one place.

## The four promises, against the evidence

Confluent's quick start describes the Streamhouse as four solutions.
Here's how each one held up.

### 1. Maintain one consistent state

This held, with a catch about timing.
`passenger_state` was defined once.
The bounded join computed the same connection times, the Lightning reads and the agent saw the same passengers and risk flags, and a delay flipped two of them to `AT_RISK` without anyone touching the definition ([part 3](03-materialized-tables-in-practice.md)).
The catch is how that state moves.
Derived state reached readers in steps about a minute apart, because Flink commits its exactly-once output about once a minute, [as Confluent documents](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html), and Lightning took another 3 to 7 seconds after each commit ([part 6](06-how-fresh-is-real-time.md)).
Every reader agrees on the same state, and they all agree on it about a minute late.

### 2. Decide across the last five seconds and the last five years

This works, as long as "seconds" means tens of seconds.
The recent side took 18 to 68 seconds, about 42 on average, from a source write to a Lightning read of the derived state.
That's real time for an operations team and slow for a checkout page.
The history side worked well.
Tableflow put the data in Iceberg about 4 minutes after it arrived, pyiceberg read it directly, and after I deleted the Kafka copy, one Flink snapshot query returned the old rows from Iceberg together with new rows that existed only in Kafka ([part 4](04-proving-tableflow-wrote-iceberg.md)).
That hybrid read is how one query covers both the last few seconds and the years before.
I didn't query the Iceberg tables from an external analytics engine at scale.

### 3. Ground and adapt an AI agent

This held, and it impressed me most.
A Flink agent on a managed Claude model looked up live flights and hotels through the Real-Time Context Engine and gave exactly the expected rebooking offers on its first run ([part 5](05-a-streaming-agent-grounded-in-live-state.md)).
I didn't have to set up a model endpoint, put an API key in SQL, or build a retrieval pipeline.
The model ignored the one-line answer format, so anything reading its output has to parse defensively.
And on a Basic cluster the agent's credential could administer the cluster, because read-only roles aren't available there ([part 2](02-basic-clusters-and-read-only-agents.md)).

### 4. Monitor the business

This held at small scale.
The risk flag is a business monitor that keeps itself up to date.
Flink does mark the quick start's own query with two CRITICAL warnings, though: the primary key doesn't match the derived upsert key, and the joins keep state forever.
With three passengers the warnings had no visible effect; at real volume someone has to address both during design review.

## Where it fits

Based on what the lab showed, a Streamhouse on Confluent Cloud fits well when:

- The business already runs on Kafka, and the hard part is keeping derived state consistent across many consumers.
- Decisions need current state, and a delay of tens of seconds is acceptable: operations dashboards, exception handling like rebooking, alerting, and agents acting on business state.
- AI agents need live, governed context and you'd rather not build a retrieval stack.
- The team would rather configure managed services than run a serving database, a cache, and an export pipeline.

## Where it does not, yet

- Sub-second freshness, or users reading their own writes, with the default exactly-once delivery I tested. Flink commits derived state about once a minute and Lightning adds seconds on top, so someone who just changed something won't see it in derived state yet. Confluent says it is working on reducing that latency.
- Analytical queries on the serving layer. Lightning returns at most 200 rows per MCP query and rejects aggregates, `GROUP BY`, and joins, so that work belongs in Flink or in an engine reading Iceberg.
- High-rate lookups on large tables, until someone measures them. On 10,000 rows each query scanned about 1.4 MB on average, which looks like most of the table, and Lightning bills compute partly by bytes scanned. If that grows with table size, heavy point-lookup traffic could get expensive. I'd test this before committing.
- Least privilege on the cheapest cluster. If agents must hold read-only credentials, budget for a Standard cluster.
- Clouds other than AWS. Lightning Tables and the Real-Time Context Engine are AWS only.

## The cost shape

You pay for what runs:

- A materialized table costs at least one Flink CFU for as long as it exists, whether or not data arrives.
- A Tableflow topic costs by the hour while it's enabled.
- Lightning Tables always cost storage, and cost compute when data arrives or queries scan.
- The agent costs per run.

An idle Basic cluster, compute pool, and Schema Registry cost nothing, which is why I could build everything in code and tear it down between sessions.

## What I would still want to know

- Whether the commit interval can be shortened for a statement, and what that does to CFU use. The documentation describes no setting for it and points to support.
- How scanned bytes and latency change at 100,000 rows and beyond, measured from inside the region.
- What evolution and failure look like: changing a materialized table in place, stopping it under load, breaking a schema.
- What each of those actually costs on the invoice.

## Would I use it?

I think the Streamhouse idea holds up.
You define the current state once, Flink keeps it up to date, applications and agents read it, and Iceberg keeps its history, with Kafka topics as the only interface between layers.
On Confluent Cloud it takes surprisingly little code, and the streaming agent went from SQL to a correct answer on the first try.

In this configuration, though, "real time" meant tens of seconds, and Flink's exactly-once commit interval set that, not the serving layer.
Lightning is good for fast lookups and can't aggregate or join.
I'd use it for operational decisions and agent context where a minute is fresh enough, measure scan costs on my own data before depending on high-rate lookups, and keep analytical work in Iceberg.
