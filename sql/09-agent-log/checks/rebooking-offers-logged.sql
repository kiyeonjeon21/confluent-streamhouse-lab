-- lab:mode=snapshot
-- The agent check from sql/06-agent, with rebooking_agent_log as the system log table.
-- Each execution runs the agent once per request and is billed. Used by experiment E5.
SELECT q.passenger_id, r.agent_status, r.response
FROM rebooking_requests AS q,
LATERAL TABLE(AI_RUN_AGENT(
  'rebooking_agent',
  CONCAT('passenger_id=', q.passenger_id,
         '; final_destination=', q.final_destination,
         '; inbound_arrival=', CAST(q.inbound_arrival AS STRING)),
  q.passenger_id,
  'rebooking_agent_log'
)) AS r(agent_status, response);
