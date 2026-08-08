-- Agent auto-investigation audit trail.
-- Stores MooDeng's background investigations triggered by alerts and exceptions.

CREATE TABLE IF NOT EXISTS agent_investigations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,

    -- What triggered the investigation
    trigger_type VARCHAR(20) NOT NULL,   -- 'alert', 'exception', 'regression'
    trigger_ref VARCHAR(255),            -- alert rule ID or exception fingerprint
    trigger_summary TEXT NOT NULL,        -- human-readable description of what fired

    -- Investigation result
    status VARCHAR(20) NOT NULL DEFAULT 'running',  -- 'running', 'completed', 'failed'
    findings TEXT,                        -- MooDeng's conclusion
    tool_calls_log JSONB DEFAULT '[]',   -- audit of every tool call made
    model_used VARCHAR(100),
    tokens_used INTEGER DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT chk_investigation_trigger_type
        CHECK (trigger_type IN ('alert', 'exception', 'regression')),
    CONSTRAINT chk_investigation_status
        CHECK (status IN ('running', 'completed', 'failed'))
);

CREATE INDEX idx_agent_investigations_project
    ON agent_investigations(project_id, created_at DESC);

CREATE INDEX idx_agent_investigations_cooldown
    ON agent_investigations(project_id, trigger_ref, created_at DESC)
    WHERE status = 'running';
