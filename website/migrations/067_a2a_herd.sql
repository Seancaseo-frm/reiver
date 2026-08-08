-- Herd: A2A Agent Registry and Message Hub
-- Postgres tables for agent registry, cross-org access grants, and push notification configs.
-- ClickHouse tables (a2a_tasks, a2a_messages, a2a_request_log) are managed separately.

-- Registered A2A agents
CREATE TABLE a2a_agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    agent_card JSONB NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'org'
        CHECK (visibility IN ('private', 'org', 'public')),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_a2a_agents_org ON a2a_agents (organization_id);
CREATE INDEX idx_a2a_agents_visibility ON a2a_agents (visibility) WHERE enabled = true;

-- Cross-org access grants
CREATE TABLE a2a_access_grants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_agent_id UUID NOT NULL REFERENCES a2a_agents(id) ON DELETE CASCADE,
    target_org_id UUID NOT NULL,
    granted_org_id UUID,
    granted_agent_id UUID,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'denied', 'revoked')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    resolved_by UUID,
    CHECK (granted_org_id IS NOT NULL OR granted_agent_id IS NOT NULL)
);

CREATE INDEX idx_a2a_access_grants_target ON a2a_access_grants (target_agent_id, status);
CREATE INDEX idx_a2a_access_grants_granted_org ON a2a_access_grants (granted_org_id, status);

-- Push notification configs (per-task webhook registrations)
CREATE TABLE a2a_push_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id TEXT NOT NULL,
    agent_id UUID NOT NULL REFERENCES a2a_agents(id) ON DELETE CASCADE,
    webhook_url TEXT NOT NULL,
    auth_scheme TEXT,
    auth_credentials TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_a2a_push_configs_task ON a2a_push_configs (task_id);
