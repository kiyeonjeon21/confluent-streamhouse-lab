-- A managed model that runs inside Confluent Cloud, so it needs no endpoint or key.
-- Run SHOW MODELS to confirm the model ID is offered in the lab's region.
-- Adapted from the Confluent Streamhouse quick start, Step 6.
CREATE MODEL rebooking_model
INPUT (prompt STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'confluent',
  'task' = 'text_generation',
  'confluent.model' = 'anthropic.claude-sonnet-4-6'
);
