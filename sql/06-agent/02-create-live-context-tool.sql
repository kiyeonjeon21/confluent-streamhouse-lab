-- Exposes two Real-Time Context Engine MCP tools to the agent.
-- `uv run lab agent-connection create` must create rtce-connection first.
-- Adapted from the Confluent Streamhouse quick start, Step 6.
CREATE TOOL river_air_live_context
USING CONNECTION `rtce-connection`
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'getMetadata,queryData',
  'request_timeout' = '30'
);
