-- Prevent duplicate pending access requests for the same (org, agent) pair.
CREATE UNIQUE INDEX idx_a2a_access_grants_unique_pending
    ON a2a_access_grants (granted_org_id, target_agent_id)
    WHERE status = 'pending';

-- Index for the hot-path grant lookup in resolve_and_check_access.
CREATE INDEX idx_a2a_access_grants_org_agent_time
    ON a2a_access_grants (granted_org_id, target_agent_id, requested_at DESC);

-- Index for list_incoming queries by target org.
CREATE INDEX idx_a2a_access_grants_target_org_status
    ON a2a_access_grants (target_org_id, status, requested_at DESC);
