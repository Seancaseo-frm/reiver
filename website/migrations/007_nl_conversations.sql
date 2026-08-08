-- Natural language query conversations for multi-turn Text-to-SQL.
-- Each conversation belongs to a project+user and contains an ordered
-- sequence of question/SQL turns.

CREATE TABLE IF NOT EXISTS nl_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_nl_conversations_project ON nl_conversations(project_id);
CREATE INDEX idx_nl_conversations_user ON nl_conversations(user_id);

CREATE TABLE IF NOT EXISTS nl_conversation_turns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES nl_conversations(id) ON DELETE CASCADE,
    turn_index INT NOT NULL,
    question TEXT NOT NULL,
    generated_sql TEXT NOT NULL,
    execution_time_ms INT,
    row_count INT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(conversation_id, turn_index)
);

CREATE INDEX idx_nl_turns_conversation ON nl_conversation_turns(conversation_id);
