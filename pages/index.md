---
title: Building a Streamhouse on Confluent Cloud
description: A hands-on series on Kafka, Flink materialized tables, Lightning Tables, Tableflow, and a streaming agent on Confluent Cloud, with measured results.
layout: landing
---

<header class="hero">
  <p class="kicker">Field report &middot; 7 parts &middot; November 2026</p>
  <h1>How real-time is a Streamhouse?</h1>
  <p class="lede">
    I rebuilt Confluent's Streamhouse quick start as code, tested every capability, and measured it.
    It works, including a streaming AI agent and Iceberg history in the same query as live data.
    But on this setup "real time" means about 45 seconds, and most of that is waiting for one Flink commit.
  </p>
</header>

<div class="stats">
  <div class="stat">
    <span class="value">44 s</span>
    <span class="label">median from a source write to a Lightning read of derived state (n = 30)</span>
  </div>
  <div class="stat">
    <span class="value">~1 min</span>
    <span class="label">between Flink's output commits; waiting for one is most of that time</span>
  </div>
  <div class="stat">
    <span class="value">3 to 7 s</span>
    <span class="label">for Lightning to serve committed output; the docs say 100 ms</span>
  </div>
  <div class="stat">
    <span class="value">1 query</span>
    <span class="label">returned rows only in Iceberg and rows only in Kafka: the hybrid read works</span>
  </div>
</div>

<div class="paths">
  <a class="path" href="{{ '/07-is-a-streamhouse-worth-it.html' | relative_url }}">
    <span class="who">Deciding whether to use it</span>
    Where it fits, where it does not, and what it costs.
    <span class="go">Read part 7 &rarr;</span>
  </a>
  <a class="path" href="{{ '/01-the-lab.html' | relative_url }}">
    <span class="who">Building one</span>
    The architecture, the lab, and what to check before you trust the docs.
    <span class="go">Start at part 1 &rarr;</span>
  </a>
</div>

## The series

<ol class="posts">
  <li>
    <span class="meta">Part 1 &middot; Build</span>
    <a class="title" href="{{ '/01-the-lab.html' | relative_url }}">The lab: a Streamhouse in Terraform, SQL files, and tests</a>
    <p>What a Streamhouse is made of, how the lab is built, and what passed.</p>
  </li>
  <li>
    <span class="meta">Part 2 &middot; Permissions</span>
    <a class="title" href="{{ '/02-basic-clusters-and-read-only-agents.html' | relative_url }}">Basic clusters cannot hold a read-only agent</a>
    <p>Why the cheapest cluster type and least privilege for agents turned out to be mutually exclusive.</p>
  </li>
  <li>
    <span class="meta">Part 3 &middot; Flink</span>
    <a class="title" href="{{ '/03-materialized-tables-in-practice.html' | relative_url }}">Materialized tables in practice</a>
    <p>The continuous query you cannot see in the statement list, and two CRITICAL warnings on the official example.</p>
  </li>
  <li>
    <span class="meta">Part 4 &middot; Tableflow</span>
    <a class="title" href="{{ '/04-proving-tableflow-wrote-iceberg.html' | relative_url }}">Proving that Tableflow wrote Iceberg</a>
    <p>Why a matching Flink result proves nothing, reading Iceberg directly, and proving the hybrid read.</p>
  </li>
  <li>
    <span class="meta">Part 5 &middot; Agents</span>
    <a class="title" href="{{ '/05-a-streaming-agent-grounded-in-live-state.html' | relative_url }}">A streaming agent grounded in live state</a>
    <p>A Flink agent on a managed Claude model, reading Lightning Tables over MCP, right on the first run.</p>
  </li>
  <li>
    <span class="meta">Part 6 &middot; Performance</span>
    <a class="title" href="{{ '/06-how-fresh-is-real-time.html' | relative_url }}">How fresh is "real time"?</a>
    <p>Where the 44 seconds go, query latency and concurrency, and the benchmark bug that made the first run look slower.</p>
  </li>
  <li>
    <span class="meta">Part 7 &middot; Verdict</span>
    <a class="title" href="{{ '/07-is-a-streamhouse-worth-it.html' | relative_url }}">Is a Streamhouse worth it?</a>
    <p>The four promises against the evidence, the alternatives, where it fits, and where it does not.</p>
  </li>
</ol>

## How it was measured

<p class="built">
  Everything was built with Terraform, version-controlled Flink SQL, and about 140 automated tests, then torn down and rebuilt between sessions.
  Every number comes from a recorded run on 2026-11-15: AWS <code>us-east-1</code>, a Basic cluster, 10,000 generated passengers, and a client in Seoul about 190 ms away.
  Each post states its method, sample size, and what it could not show.
</p>
