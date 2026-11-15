---
title: Building a Streamhouse on Confluent Cloud
description: A hands-on series on Kafka, Flink materialized tables, Lightning Tables, Tableflow, and a streaming agent on Confluent Cloud, with measured results.
layout: landing
---

# How real-time is a Streamhouse?

In November 2026 I rebuilt River Air, Confluent's Streamhouse quick start, as Terraform and SQL files, tested each piece, and then measured it from my laptop in Seoul against AWS `us-east-1`.

Most of it worked the first time, including the streaming agent and a single Flink query that reads Iceberg history and live Kafka data together.
Freshness was the surprise.
Across 30 samples, a change took a median of 44 seconds to show up in derived state, and nearly all of that was waiting for Flink to commit its output, which it does about once a minute.
After the commit, Lightning Tables took another 3 to 7 seconds, even though the documentation says a Lightning Table updates within 100 ms of new data arriving in its topic.

If you're deciding whether to use a Streamhouse, part 7 sums it up.
If you want to build one, start with part 1.

## The series

1. [The lab: a Streamhouse in Terraform, SQL files, and tests](01-the-lab.md).
   What a Streamhouse is made of, how I built the lab, and what passed.
2. [Basic clusters cannot hold a read-only agent](02-basic-clusters-and-read-only-agents.md).
   On the cheapest cluster type, the agent's credential has to be an admin role.
3. [Materialized tables in practice](03-materialized-tables-in-practice.md).
   The continuous query that doesn't appear in the statement list, and two CRITICAL warnings on the official example.
4. [Proving that Tableflow wrote Iceberg](04-proving-tableflow-wrote-iceberg.md).
   Reading the Iceberg table directly, and deleting the Kafka copy to prove the hybrid read.
5. [A streaming agent grounded in live state](05-a-streaming-agent-grounded-in-live-state.md).
   A Flink agent on a managed Claude model reads Lightning Tables over MCP and gets the answer right on its first run.
6. [How fresh is "real time"?](06-how-fresh-is-real-time.md)
   Where the 44 seconds go, query latency and concurrency, and a benchmark bug that made my first run look slower.
7. [Is a Streamhouse worth it?](07-is-a-streamhouse-worth-it.md)
   The four promises against what I measured, where it fits, and where it doesn't.

## How I measured

I built everything with Terraform, version-controlled Flink SQL, and about 140 automated tests, and tore it down between sessions.
The numbers come from runs on 2026-11-15 on AWS `us-east-1` with a Basic cluster and 10,000 generated passengers, measured from Seoul, about 190 ms away.
Each post gives its method, its sample size, and what it couldn't show.
