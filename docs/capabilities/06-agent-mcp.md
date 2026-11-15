# Capability 6: AI agent access through MCP

## Architecture and purpose

A Streaming Agent in Flink runs on a managed model (Claude Sonnet 4.6, hosted in Confluent Cloud) and calls the Real-Time Context Engine through an MCP tool.
For each at-risk passenger it looks up live flights and hotels and returns two rebooking offers and an optional hotel.
The same engine is reachable from any MCP client, so an external agent such as Claude Code reads exactly the state the in-Flink agent reads.

```mermaid
sequenceDiagram
  participant Q as AI_RUN_AGENT (snapshot query)
  participant A as rebooking_agent
  participant M as rebooking_model (managed)
  participant R as Real-Time Context Engine
  participant L as Lightning Tables
  Q->>A: passenger_id, final_destination, inbound_arrival
  A->>M: prompt + tool definitions
  M->>A: call queryData(flight_status, ...)
  A->>R: MCP queryData (rtce-connection, context-reader key)
  R->>L: SELECT on flight_status
  L-->>A: rows
  A->>M: rows, then hotel_inventory the same way
  M-->>Q: OFFER1=...;OFFER2=...;HOTEL=...
```

## Deployment steps

```sh
uv run lab agent-connection create
uv run lab apply agent
```

Requires capability 4 with `flight_status` and `hotel_inventory` as Lightning Tables, and the RA417 delay event, so that `rebooking_requests` gets the at-risk passengers.

## SQL, configuration, and API examples

- [`sql/06-agent`](../../sql/06-agent/): model, tool, agent, request table, request insert, and the `AI_RUN_AGENT` check.
- The MCP connection is created with the CLI ([`agent_connection.py`](../../src/streamhouse_lab/agent_connection.py)) because Terraform's `confluent_flink_connection` has no `MCP_SERVER` type in provider 2.95.0.
- External client (do not paste the token into chat or commit it):
  ```sh
  claude mcp add --transport http confluent-rtce "$(terraform -chdir=terraform output -raw context_mcp_endpoint)" \
    --header "Authorization: Basic <base64 of key:secret>"
  ```

## Test procedure

```sh
uv run lab check agent
scripts/verify-live capability_6
```

Each run is billed for model inference and MCP queries.

## Expected versus actual results

Expected: `P-001` and `P-002` both `SUCCESS` with `OFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel`.
RA214 is skipped because it leaves only 40 minutes after landing; the hotel is added because RA232 leaves the next morning.
The model may add a sentence before the offer line.

Actual: **not run yet**.

## Issues, limitations, and lessons learned

Known before running:

- `AI_RUN_AGENT` reads only append-only input.
- The CLI takes the connection's secret as an argument, so it is briefly visible in the local process table.
- Open question: on a Basic cluster, is the context reader's topic-scoped `DeveloperRead` binding accepted?
  If Terraform rejects it, the documented alternative is cluster-scoped `DeveloperRead`, which also grants all three MCP operations.
- Open question: the quick start creates the Global key for the user account. Does a service account's Global key work identically for the Flink connection?

To record after the first run:

- Agent latency per passenger and number of tool calls.
- Whether wording varies across runs and whether the offer line stays parseable.
