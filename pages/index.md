---
title: Building a Streamhouse on Confluent Cloud
description: A hands-on series on Kafka, Flink materialized tables, Lightning Tables, Tableflow, and a streaming agent, with measured results.
---

Confluent describes a Streamhouse as an architecture that keeps the current state of the business continuously computed and serves that one state to applications, AI agents, and analytics.
I rebuilt Confluent's official River Air quick start as a reproducible lab, verified every capability with automated tests, and then measured how fresh and how fast it really is.

This series is what I learned.
Every number in it comes from a real run on 2026-11-15, and each post says how it was measured.

## The posts

### 1. [The lab: a Streamhouse in Terraform, SQL files, and tests](01-the-lab.md)

What a Streamhouse is made of, how the lab is built, and what passed.

### 2. [Basic clusters cannot hold a read-only agent](02-basic-clusters-and-read-only-agents.md)

The permission model that made the cheapest cluster type and least privilege mutually exclusive.

### 3. [Materialized tables in practice](03-materialized-tables-in-practice.md)

The continuous query you cannot see in the statement list, and two CRITICAL warnings on the official example.

### 4. [Proving that Tableflow wrote Iceberg](04-proving-tableflow-wrote-iceberg.md)

Why a matching Flink result proves nothing, and how to read the Iceberg table directly.

### 5. [A streaming agent grounded in live state](05-a-streaming-agent-grounded-in-live-state.md)

A Flink agent on a managed Claude model, reading Lightning Tables over MCP.

### 6. [How fresh is "real time"?](06-how-fresh-is-real-time.md)

Freshness, query latency, and concurrency, plus the benchmark bug that made the first run look twice as slow.

### 7. [Is a Streamhouse worth it?](07-is-a-streamhouse-worth-it.md)

The promises against the evidence, the alternatives, where it fits, and where it does not.

## At a glance

| Capability | Result |
|---|---|
| Kafka source tables, Flink joins, a materialized table | Matched the quick start exactly |
| Lightning Tables over MCP | Worked, but on a Basic cluster only with an admin role |
| Tableflow to Iceberg | Rows reached Iceberg about 4.5 minutes after enabling |
| Streaming agent | Both at-risk passengers got the expected rebooking offers |
| Freshness, source change to Lightning read | 18 to 68 s, median 46 s (n = 30) |
| Lightning query latency from Seoul to us-east-1 | About 690 ms median, of which about 190 ms is network |
| Concurrency | Linear to 16 clients, 40.6 req/s at 32, no errors |

The code, SQL, and test suite are in the `confluent-streamhouse-lab` repository.
