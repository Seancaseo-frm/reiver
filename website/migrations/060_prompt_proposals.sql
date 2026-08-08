CREATE TABLE llm_prompt_proposals (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    config_id       UUID NOT NULL REFERENCES llm_prompt_configs(id) ON DELETE CASCADE,

    -- Candidate prompt version fields (mirrors llm_prompt_versions)
    system_prompt   TEXT,
    model           VARCHAR(100),
    temperature     DECIMAL(3,2) NOT NULL DEFAULT 0.5,
    max_tokens      INT,
    parameters      JSONB DEFAULT '{}',
    variables       JSONB DEFAULT '[]',
    tools           JSONB,
    response_format JSONB,
    allowed_tools   JSONB,

    -- Proposal metadata
    reasoning       TEXT NOT NULL,
    comparison      JSONB NOT NULL DEFAULT '{}',
    session_ids     TEXT[] NOT NULL DEFAULT '{}',
    proposed_by     VARCHAR(10) NOT NULL DEFAULT 'agent',
    task_id         UUID,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prompt_proposals_project ON llm_prompt_proposals(project_id);
CREATE INDEX idx_prompt_proposals_config  ON llm_prompt_proposals(config_id);
