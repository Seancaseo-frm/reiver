-- MCP credit log for tracking per-action credit consumption.
-- Used by free-tier enforcement to hard-cap credits without relying on ClickHouse.

CREATE TABLE IF NOT EXISTS mcp_credit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    project_id UUID NOT NULL REFERENCES projects(id),
    tool_name TEXT NOT NULL,
    credits INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mcp_credit_log_org_period
    ON mcp_credit_log (organization_id, created_at);

CREATE UNIQUE INDEX idx_mcp_credit_log_idempotency
    ON mcp_credit_log (idempotency_key);
