-- Drop event subscription tables (events are now internal-only with hardcoded routing)
DROP TABLE IF EXISTS event_subscription_executions;
DROP TABLE IF EXISTS event_subscriptions;

-- Agent tasks: audit trail for headless MooDeng tasks (separate from investigations)
CREATE TABLE IF NOT EXISTS agent_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_type VARCHAR(100) NOT NULL,
    task_ref TEXT NOT NULL,
    prompt TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    result TEXT,
    tool_calls_log JSONB,
    model_used VARCHAR(100),
    tokens_used INT,
    internal BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_agent_tasks_project ON agent_tasks(project_id);
CREATE INDEX idx_agent_tasks_running ON agent_tasks(project_id, task_ref)
    WHERE status = 'running';
