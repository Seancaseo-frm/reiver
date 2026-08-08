-- Make access grants agent-to-agent: granted_agent_id becomes required.

-- Backfill existing rows: pick the first enabled agent in the requesting project.
UPDATE a2a_access_grants g
SET granted_agent_id = (
    SELECT a.id FROM a2a_agents a
    WHERE a.project_id = g.requesting_project_id AND a.enabled = true
    ORDER BY a.created_at ASC
    LIMIT 1
)
WHERE g.granted_agent_id IS NULL;

-- Delete any rows that couldn't be backfilled (orphaned grants with no agents).
DELETE FROM a2a_access_grants WHERE granted_agent_id IS NULL;

ALTER TABLE a2a_access_grants
    ALTER COLUMN granted_agent_id SET NOT NULL;

-- Drop old org-level unique index and create agent-to-agent one.
DROP INDEX IF EXISTS idx_a2a_access_grants_unique_pending;
CREATE UNIQUE INDEX idx_a2a_access_grants_unique_pending
    ON a2a_access_grants (granted_agent_id, target_agent_id)
    WHERE status = 'pending';

-- Update the hot-path lookup index from org-based to agent-based.
DROP INDEX IF EXISTS idx_a2a_access_grants_org_agent_time;
CREATE INDEX idx_a2a_access_grants_agent_pair_time
    ON a2a_access_grants (granted_agent_id, target_agent_id, requested_at DESC);
