-- lab:mode=snapshot
-- Runs the agent once per request. Each run calls the managed model and the MCP tools,
-- so every execution of this check is billed.
-- Adapted from the Confluent Streamhouse quick start, Step 6.
SELECT q.passenger_id, r.agent_status, r.response
FROM rebooking_requests AS q,
LATERAL TABLE(AI_RUN_AGENT(
  'rebooking_agent',
  CONCAT('passenger_id=', q.passenger_id,
         '; final_destination=', q.final_destination,
         '; inbound_arrival=', CAST(q.inbound_arrival AS STRING)),
  q.passenger_id
)) AS r(agent_status, response);
