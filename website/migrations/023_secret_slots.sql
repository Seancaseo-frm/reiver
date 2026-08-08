-- Secret slots: single-use, time-limited opaque references for depositing
-- secrets without exposing them to the AI agent's LLM context.

CREATE TABLE IF NOT EXISTS secret_slots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    created_by      UUID NOT NULL,
    purpose         TEXT NOT NULL,
    provider        TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'filled', 'consumed', 'expired')),
    encrypted_value TEXT,
    expires_at      TIMESTAMPTZ NOT NULL,
    filled_at       TIMESTAMPTZ,
    consumed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_secret_slots_id_status ON secret_slots (id, status);
CREATE INDEX IF NOT EXISTS idx_secret_slots_expires ON secret_slots (expires_at) WHERE status = 'pending';
