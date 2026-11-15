---
title: A streaming agent grounded in live state
description: A Flink agent on a managed Claude model reads Lightning Tables through the Real-Time Context Engine and rebooks passengers who miss connections.
date: 2026-11-15
---

*Part 5 of [Building a Streamhouse on Confluent Cloud](index.md).*

## The pieces

The rebooking agent is three Flink SQL objects and one connection:

```sql
CREATE MODEL rebooking_model
INPUT (prompt STRING) OUTPUT (response STRING)
WITH ('provider' = 'confluent', 'task' = 'text_generation',
      'confluent.model' = 'anthropic.claude-sonnet-4-6');

CREATE TOOL river_air_live_context
USING CONNECTION `rtce-connection`
WITH ('type' = 'mcp',
      'allowed_tools' = 'getMetadata,queryData',
      'request_timeout' = '30');

CREATE AGENT rebooking_agent
USING MODEL rebooking_model
USING PROMPT '... offer the first two flights from SFO to the final
destination that leave at least 45 minutes after arrival, skip CANCELLED
flights, and add the first hotel alphabetically with rooms if an offer
leaves 6 or more hours later ... Reply with exactly one line:
OFFER1=<flight_id>;OFFER2=<flight_id>;HOTEL=<hotel or NONE>'
USING TOOLS river_air_live_context
WITH ('max_iterations' = '10');
```

- The **model** is managed: it runs inside Confluent Cloud, so it needs no endpoint or API key.
  `SHOW MODELS` in `us-east-1` listed Claude Sonnet 4.6, Opus 4.6, 4.7 and 4.8, Haiku 4.5, three Amazon Nova models, and two others.
- The **tool** is the Real-Time Context Engine's MCP server, restricted to `getMetadata` and `queryData`.
- The **connection** carries the MCP endpoint and the context reader's Global API key.
  It is the one object Terraform cannot create yet, so the lab uses `confluent flink connection create`.

## Feeding it

`AI_RUN_AGENT` reads only append-only input, and `passenger_state` is an upsert table.
So at-risk passengers are copied into an append-only `rebooking_requests` table, and the agent runs over that:

```sql
SELECT q.passenger_id, r.agent_status, r.response
FROM rebooking_requests AS q,
LATERAL TABLE(AI_RUN_AGENT(
  'rebooking_agent',
  CONCAT('passenger_id=', q.passenger_id,
         '; final_destination=', q.final_destination,
         '; inbound_arrival=', CAST(q.inbound_arrival AS STRING)),
  q.passenger_id
)) AS r(agent_status, response);
```

## The result

```
P-001  SUCCESS
  The first available hotel alphabetically is **Harbor Point Hotel**.

  OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel

P-002  SUCCESS
  The first hotel alphabetically with available rooms is **Harbor Point Hotel**.

  OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel
```

This is exactly the quick start's expected answer.
The agent skipped RA214, which now leaves only 40 minutes after the delayed arrival.
It added a hotel because RA232 leaves the next morning, and it skipped the sold-out Airport Bayfront Inn.
All of that came from live Lightning reads, not from anything in the prompt.

## Notes from running it

- **The model did not reply with exactly one line**, in either run, despite the prompt.
  It prefixed a sentence.
  The quick start warns about this.
  Anything that consumes the response should search for the `OFFER1=` line, not parse the field; the lab's test does that.
- **One run for two passengers took 24 to 26 seconds** end to end, including the snapshot query start-up, model calls, and MCP calls.
  The result does not report how many tool calls the agent made, so per-step timing needs another source.
- **The agent's credential is broader than its tools.**
  The MCP tools are read-only, but on a Basic cluster the context reader's key holds `CloudClusterAdmin` ([part 2](02-basic-clusters-and-read-only-agents.md)).
- **The secret stays out of SQL history**, which is the point of creating the connection with the CLI, and `confluent flink connection describe` does not return it.
  The CLI does take it as a command-line argument, so it is briefly visible in the local process list.

## Why this matters

The same Lightning Tables that the agent reads are open to any MCP client.
Point Claude Code at the same endpoint and it sees the same passenger state the in-Flink agent saw.
That is the Streamhouse argument in one sentence: one continuously maintained state, read by applications, analytics, and agents alike.

Next: [How fresh is "real time"?](06-how-fresh-is-real-time.md).
