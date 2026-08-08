-- Platform event subscription system.
-- Allows users to subscribe to platform events (alerts, exceptions, feature flags, etc.)
-- and trigger actions (webhooks, notification channels, AI agent tasks).

CREATE TABLE IF NOT EXISTS event_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- What to listen for
    event_types TEXT[] NOT NULL,            -- e.g. {'alert_fired', 'exception_group_regressed'}
    filter_condition JSONB,                 -- optional payload filter (future use)

    -- What to do
    action_type VARCHAR(30) NOT NULL,       -- 'webhook', 'notification_channel', 'agent_task', 'http_callback'
    action_config JSONB NOT NULL,           -- type-specific configuration

    -- Execution control
    cooldown_seconds INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,

    -- Audit
    created_by UUID REFERENCES users(id),
    updated_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_sub_action_type
        CHECK (action_type IN ('webhook', 'notification_channel', 'agent_task', 'http_callback'))
);

CREATE INDEX idx_event_subs_project
    ON event_subscriptions(project_id)
    WHERE enabled = true;

CREATE INDEX idx_event_subs_types
    ON event_subscriptions USING GIN(event_types);

CREATE TABLE IF NOT EXISTS event_subscription_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES event_subscriptions(id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    result JSONB,
    error TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_exec_status
        CHECK (status IN ('pending', 'running', 'completed', 'failed'))
);

CREATE INDEX idx_event_exec_sub
    ON event_subscription_executions(subscription_id, created_at DESC);

CREATE INDEX idx_event_exec_cooldown
    ON event_subscription_executions(subscription_id, created_at DESC)
    WHERE status IN ('running', 'completed');
