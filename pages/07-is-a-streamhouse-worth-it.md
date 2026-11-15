---
title: Is a Streamhouse worth it?
description: What a Streamhouse promises, what this lab actually showed for each promise, where it fits, and where it does not.
date: 2026-11-15
part: 7
lang: en
alternate_url: /ko/07-is-a-streamhouse-worth-it.html
translation_key: chapter-07
reading_time: 11
sample: Evidence from all 6 capabilities and E1 to E6
takeaways:
  - label: "Observed"
    text: "One Flink definition of passenger state fed both the Lightning reads and the agent, while Tableflow preserved history without another export pipeline."
  - label: "Interpretation"
    text: "As tested, with default exactly-once delivery, it fits operational decisions that can wait tens of seconds. It doesn't fit sub-second read-your-write paths or analytics on the serving layer."
  - label: "Limitation"
    text: "Point lookups scan the table, so test scan cost at your table size, use a cluster type that gives agent credentials least privilege, and treat a query change as a rebuild."
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
| Streamhouse on Confluent Cloud | Flink materialized tables over Kafka topics; Lightning Tables serve them; Tableflow writes Iceberg | Measured here: 7 to 68 s | Iceberg, from the same topics | Mostly configuration; the parts are managed |

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
Derived state reached readers in steps about a minute apart, because Flink commits its exactly-once output about once a minute, [as Confluent documents](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html), and Lightning took about 5 seconds more to serve each commit ([part 6](06-how-fresh-is-real-time.md)).
Every reader agrees on the same state, and they all agree on it about a minute late.

### 2. Decide across the last five seconds and the last five years

This works, as long as "seconds" means tens of seconds.
The recent side took 7 to 68 seconds, about 41 on average, from a source write to a Lightning read of the derived state, the same from Seoul at 10,000 rows as from inside the region at 100,000.
That's real time for an operations team and slow for a checkout page.
The history side worked well.
Tableflow put the data in Iceberg about 4 minutes after it arrived, pyiceberg read it directly, and after I deleted the Kafka copy, one Flink snapshot query returned the old rows from Iceberg together with new rows that existed only in Kafka ([part 4](04-proving-tableflow-wrote-iceberg.md)).
That hybrid read is how one query covers both the last few seconds and the years before.
I didn't query the Iceberg tables from an external analytics engine at scale.

### 3. Ground and adapt an AI agent

This held, and it impressed me most.
A Flink agent on a managed Claude model looked up live flights and hotels through the Real-Time Context Engine and gave exactly the expected rebooking offers on its first run ([part 5](05-a-streaming-agent-grounded-in-live-state.md)).
When I changed a flight under it twenty times, it answered from the new data every time, and Claude Code with the same model and the same two tools did too.
I didn't have to set up a model endpoint, put an API key in SQL, or build a retrieval pipeline.
The model ignored the one-line answer format, so anything reading its output has to parse defensively.
And on a Basic cluster the agent's credential could administer the cluster, because read-only roles aren't available there ([part 2](02-basic-clusters-and-read-only-agents.md)).

### 4. Monitor the business

This held at small scale.
The risk flag is a business monitor that keeps itself up to date.
Flink does mark the quick start's own query with two CRITICAL warnings, though: the primary key doesn't match the derived upsert key, and the joins keep state forever.
With three passengers the warnings had no visible effect; at real volume someone has to address both during design review.
The bigger gap showed up when things went wrong.
When the table was stopped, failed on an unreadable record, or was changed with `CREATE OR ALTER`, Lightning kept serving its last state and stayed `ACTIVE` ([part 3](03-materialized-tables-in-practice.md)).
A monitor built on derived state needs a second monitor on the pipeline behind it.

## Where it fits

Based on what the lab showed, a Streamhouse on Confluent Cloud fits well when:

- The business already runs on Kafka, and the hard part is keeping derived state consistent across many consumers.
- Decisions need current state, and a delay of tens of seconds is acceptable: operations dashboards, exception handling like rebooking, alerting, and agents acting on business state.
- AI agents need live, governed context and you'd rather not build a retrieval stack.
- The team would rather configure managed services than run a serving database, a cache, and an export pipeline.

## Where it does not, yet

- Sub-second freshness, or users reading their own writes, with the default exactly-once delivery I tested. Flink commits derived state about once a minute and Lightning adds seconds on top, so someone who just changed something won't see it in derived state yet. Confluent says it is working on reducing that latency.
- Analytical queries on the serving layer. Lightning returns at most 200 rows per MCP query and rejects aggregates, `GROUP BY`, and joins, so that work belongs in Flink or in an engine reading Iceberg.
- High-rate lookups on large tables. Point lookups scan the table: about 1.4 MB each at 10,000 rows and 8 to 11 MB at 100,000. At 100,000 rows that came to about $0.01 per 1,000 lookups, and one engine stopped scaling at about 160 lookups a second. Measure on your own table size before you put a busy API on it.
- Changing a query on a live table. `CREATE OR ALTER` keeps the rows the new query drops and stops updating the keys the table already has, so plan a query change as a rebuild.
- Least privilege on the cheapest cluster. If agents must hold read-only credentials, budget for a Standard cluster.
- Clouds other than AWS. Lightning Tables and the Real-Time Context Engine are AWS only.

## What the lab cost

I ran everything on November 15, 2026, in two sessions in `us-east-1`, and tore the lab down after each.
The first session, 2 hours 41 minutes, covered the six capabilities and experiments E1, E3, and E4.
The second, 1 hour 7 minutes, covered E2 and the hybrid read.
The Costs API reported this for the day:

| Product | Meter | Billed | Unit price | Cost |
|---|---|---|---|---|
| Flink | CFU-hours | 6.5 | $0.21 | $1.37 |
| Kafka, Basic cluster | eCKU-hours | 8 | $0.135 | $1.08 |
| Tableflow | topic-hours | 5 | $0.10 | $0.50 |
| Lightning Tables (Real-Time Context Engine) | CCU-hours | 4.2 | $0.105 | $0.44 |
| Managed model inference for the agent | input and output tokens | about 46,000 and 3,800 | $3 and $15 per million | $0.20 |
| Kafka network | GB in and out | about 0.06 | $0.05 per GB | under $0.01 |
| **Total** | | | | **$3.59** |

The first session cost $2.59 and the second $1.00.
I read these numbers about three and a half hours after the last teardown.
Confluent's [billing FAQ](https://docs.confluent.io/cloud/current/billing/billing-faq.html) says cost data can take up to 72 hours to reach the Costs API, so more lines could still appear.

Three things are worth knowing before you budget.

Flink was the largest line.
The materialized tables run continuously, and every check and snapshot query adds CFU-minutes on top.

The Basic cluster was billed under load.
The [pricing page](https://www.confluent.io/confluent-cloud/pricing/) lists Basic's first eCKU as free, and that holds for an idle cluster: an older Basic cluster in the same organization, with six topics and no traffic, was billed nothing in October or November.
The lab's clusters were billed 8 eCKU-hours at $0.135 across the 5 UTC hours they existed in, so in some of those hours they used more than one eCKU.
The Costs API only returns daily totals, so I can't tell which hours, or which dimension (throughput, connections, or requests) pushed them past one.

Hourly meters round up.
Confluent bills any partial hour as a full UTC hour.
Tableflow shows it most clearly: the topic was enabled for 2 hours 20 minutes in the first session and billed for 3 topic-hours.

## The cost shape

Left running, the same setup would cost this by the hour at those prices:

- A materialized table uses at least one CFU for as long as it exists, whether or not data arrives: $0.21 an hour, about $5 a day.
- A Tableflow topic costs $0.10 an hour while it's enabled, $2.40 a day.
- An idle Basic cluster stays within its free first eCKU, even with topics. Each eCKU that load adds costs $0.135 an hour.
- Lightning Tables always cost storage, and cost compute when data arrives or queries scan; point lookups on 100,000 rows came to about $0.01 per 1,000.
- The agent costs per run. Its model calls for the whole day came to $0.20.

An idle Basic cluster, a compute pool with no statements, and Schema Registry Essentials cost nothing.
The expensive parts are the ones that run continuously, and tearing the lab down between sessions kept the day under $4.

A third session ran the agent comparison, the failure tests, and the 100,000-row runs.
I'll add its invoice here once the Costs API has all of it.

## What I would still want to know

- Whether the commit interval can be shortened for a statement, and what that does to CFU use. The documentation describes no setting for it and points to support.
- Whether Lightning's 5 seconds of ingest depends on the cluster type, the table, or the region; I measured one of each.
- How long rebuilding a materialized table's existing keys takes after a query change, by reprocessing the sources from the beginning, and what it costs.
- Which load pushed the Basic cluster past one eCKU. Answering that needs hourly cost data.

## Would I use it?

I think the Streamhouse idea holds up.
You define the current state once, Flink keeps it up to date, applications and agents read it, and Iceberg keeps its history, with Kafka topics as the only interface between layers.
On Confluent Cloud it takes surprisingly little code, and the streaming agent went from SQL to a correct answer on the first try.

In this configuration, though, "real time" meant tens of seconds, and Flink's exactly-once commit interval set most of that; Lightning added about 5 seconds.
Lightning answers lookups in about 34 ms from inside the region, can't aggregate or join, and scans the table to do it.
I'd use it for operational decisions and agent context where a minute is fresh enough, alert on the pipeline as well as the data, treat query changes as rebuilds, measure scan costs on my own data before depending on high-rate lookups, and keep analytical work in Iceberg.
