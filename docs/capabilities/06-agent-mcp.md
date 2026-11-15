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

Actual (2026-11-15, us-east-1, Basic cluster, `uv run lab check agent` and `scripts/verify-live capability_6`): **matches**.

```
passenger_id  agent_status  response
P-001         SUCCESS       The first available hotel alphabetically is **Harbor Point Hotel**.\n\nOFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel
P-002         SUCCESS       The first hotel alphabetically with available rooms is **Harbor Point Hotel**.\n\nOFFER1=RA226;OFFER2=RA232;HOTEL=Harbor Point Hotel
```

The full live suite, all six capabilities in order, passed in 3 min 10 s.

## Issues, limitations, and lessons learned

Observed on the first run:

- **A service account's Global API key works for the Flink MCP connection.** The quick start uses a user-owned key; the context reader's Terraform-managed key behaves the same and spends none of the user's two Global key slots.
- **The model added a sentence before the offer line in both runs**, exactly as the quick start warns. Anything that consumes the response must search for the `OFFER1=` line rather than parse the whole field; the live test does.
- **One `AI_RUN_AGENT` snapshot query for two passengers took about 26 s** end to end, including snapshot start-up, model calls, and MCP tool calls (n = 2 runs, 24 to 26 s). Per-call tool counts are not exposed in the result; Phase 4 E5 needs another source for them.
- `SHOW MODELS` in us-east-1 lists `system.anthropic.claude-sonnet-4-6`, `claude-opus-4-6`, `claude-opus-4-7`, `claude-opus-4-8`, `claude-haiku-4-5`, three Amazon Nova models, `gpt-oss-safeguard-120b`, and `titan-embed-text-v2`.
- `confluent flink connection describe -o json` does not return the password; its `data` field is a short masked value.
- The CLI still receives the secret as a command-line argument when the connection is created.

Known before running:

- `AI_RUN_AGENT` reads only append-only input, hence `rebooking_requests`.
- On a Basic cluster the context reader holds `CloudClusterAdmin` (see [capability 4](04-lightning-tables.md)), so the agent's credential is broader than read-only, even though the MCP tools it can call are read-only.

Still to try:

- Point an external MCP client (Claude Code) at the same endpoint and ask the same question, to compare an external agent with the in-Flink agent (Phase 4 E5).
