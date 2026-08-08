-- Pipeline event system: persisted event log and subscriptions

CREATE TABLE IF NOT EXISTS warehouse_pipeline_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_pipeline_events_pending
    ON warehouse_pipeline_events (status, created_at)
    WHERE status = 'pending';

CREATE INDEX idx_pipeline_events_project
    ON warehouse_pipeline_events (project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS warehouse_pipeline_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES warehouse_pipelines(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    event_filter JSONB DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(pipeline_id, event_type, event_filter)
);

CREATE INDEX idx_pipeline_subscriptions_event_type
    ON warehouse_pipeline_subscriptions (event_type)
    WHERE enabled = true;
