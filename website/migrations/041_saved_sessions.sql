CREATE TABLE saved_sessions (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    session_name TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL DEFAULT '',
    first_request_time TIMESTAMPTZ NOT NULL,
    last_request_time TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    total_input_tokens BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    total_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    models TEXT[] NOT NULL DEFAULT '{}',
    saved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, session_id)
);
