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

The model is managed, meaning it runs inside Confluent Cloud and needs no endpoint or API key.
`SHOW MODELS` in `us-east-1` listed Claude Sonnet 4.6, Opus 4.6, 4.7 and 4.8, Haiku 4.5, three Amazon Nova models, and two others.

The tool is the Real-Time Context Engine's MCP server, limited to `getMetadata` and `queryData`.

The connection holds the MCP endpoint and the context reader's Global API key.
Terraform can't create this kind of connection yet, so the lab uses `confluent flink connection create`.

## Feeding it

`AI_RUN_AGENT` reads only append-only input, and `passenger_state` is an upsert table.
So the lab copies at-risk passengers into an append-only `rebooking_requests` table and runs the agent over that:

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

That's exactly the answer the quick start expects.
The agent skipped RA214, which now leaves only 40 minutes after the delayed arrival.
It added a hotel because RA232 leaves the next morning, and it passed over the sold-out Airport Bayfront Inn.
The prompt contains no flight or hotel data, so the agent got all of this from live Lightning reads.

## Notes from running it

The model didn't reply with exactly one line in either run, despite the prompt; it put a sentence in front.
The quick start warns about this.
Anything that consumes the response should look for the `OFFER1=` line instead of parsing the whole field, which is what the lab's test does.

One run for two passengers took 24 to 26 seconds end to end, including snapshot query start-up, the model calls, and the MCP calls.
The result doesn't say how many tool calls the agent made, so timing each step would need another source.

The agent's tools are read-only, but its credential is broader.
On a Basic cluster the context reader's key holds `CloudClusterAdmin` ([part 2](02-basic-clusters-and-read-only-agents.md)).

Creating the connection with the CLI keeps the secret out of SQL history, and `confluent flink connection describe` doesn't return it.
The CLI does take the secret as a command-line argument, so it shows up briefly in the local process list.

## Other clients

Any MCP client can read the Lightning Tables; the agent inside Flink is just one of them.
Claude Code pointed at the same endpoint with the same key would read the same passenger state; I didn't run that comparison in this lab.
That shared state is what a Streamhouse is for: applications and agents work from one continuously maintained table instead of their own copies.

Next: [How fresh is "real time"?](06-how-fresh-is-real-time.md).
