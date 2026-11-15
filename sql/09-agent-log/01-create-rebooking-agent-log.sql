-- System log table for AI_RUN_AGENT. Passed as the fourth argument, it receives every
-- model call and tool call of a run, so experiment E5 can count the agent's tool calls.
-- The schema is fixed by Confluent and custom columns are not supported:
-- docs.confluent.io/cloud/current/ai/streaming-agents/monitor-streaming-agents.html
CREATE TABLE rebooking_agent_log (
  `agent_name` STRING NOT NULL,
  `job_id` STRING NOT NULL,
  `request_id` STRING NOT NULL,
  `statement_name` STRING NOT NULL,
  `iteration` INT NOT NULL,
  `type` STRING NOT NULL,
  `data` STRING NOT NULL,
  `metrics` STRING
)
WITH (
  'value.format' = 'avro-registry'
);
