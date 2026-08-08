-- Session profile match tracking: records which session profiles matched a given session
CREATE TABLE session_profile_matches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    profile_id UUID NOT NULL,
    matched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_spm_project_session_profile
    ON session_profile_matches(project_id, session_id, profile_id);
CREATE INDEX idx_spm_project_profile
    ON session_profile_matches(project_id, profile_id);

-- Tracks which sessions have already been evaluated against profiles
-- (regardless of match result) to avoid re-processing
CREATE TABLE session_evaluations (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, session_id)
);

-- Durable storage for request/response content of sessions that matched
-- a profile. Content is copied here from ClickHouse before the column TTL
-- clears it, so replay works indefinitely.
CREATE TABLE session_request_content (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_messages TEXT NOT NULL DEFAULT '',
    response_content TEXT NOT NULL DEFAULT '',
    gen_ai_request_model TEXT NOT NULL DEFAULT '',
    gen_ai_system TEXT NOT NULL DEFAULT '',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    status_code TEXT NOT NULL DEFAULT 'ok',
    timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, session_id, request_id)
);
