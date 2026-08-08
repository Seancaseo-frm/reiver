CREATE TABLE agent_attachments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    conversation_id UUID REFERENCES agent_conversations(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    storage_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_attachments_conversation ON agent_attachments(conversation_id);
CREATE INDEX idx_agent_attachments_project_user ON agent_attachments(project_id, user_id);
