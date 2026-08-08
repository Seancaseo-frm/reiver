-- Add endpoint URL and linked agent token to A2A agents.
-- endpoint_url: where Herd delivers A2A messages to the agent's application.
-- key_id: links this agent to a Flow agent token (project_keys with key_type='agent').

ALTER TABLE a2a_agents ADD COLUMN endpoint_url TEXT;
ALTER TABLE a2a_agents ADD COLUMN key_id UUID REFERENCES project_keys(id) ON DELETE SET NULL;

-- Backfill existing rows with empty endpoint (they'll need to be updated via the UI)
UPDATE a2a_agents SET endpoint_url = '' WHERE endpoint_url IS NULL;

-- Now make it NOT NULL for future inserts
ALTER TABLE a2a_agents ALTER COLUMN endpoint_url SET NOT NULL;
