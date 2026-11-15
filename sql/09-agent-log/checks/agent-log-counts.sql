-- lab:mode=snapshot
-- Model and tool calls per agent run (statement) and passenger (request), from the system log.
SELECT statement_name, request_id, `type`, COUNT(*) AS events, MAX(iteration) AS last_iteration
FROM rebooking_agent_log
GROUP BY statement_name, request_id, `type`;
