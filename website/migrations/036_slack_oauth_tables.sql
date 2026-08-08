-- Slack OAuth integration tables

-- Maps Slack threads to agent conversations for multi-turn continuity.
CREATE TABLE IF NOT EXISTS slack_thread_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    thread_ts TEXT NOT NULL,
    conversation_id UUID NOT NULL REFERENCES agent_conversations(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, channel_id, thread_ts)
);

CREATE INDEX IF NOT EXISTS idx_slack_thread_conversations_lookup
    ON slack_thread_conversations(team_id, channel_id, thread_ts);

-- Maps Slack users to Reiver users for permission-scoped agent access.
CREATE TABLE IF NOT EXISTS slack_user_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    team_id TEXT NOT NULL,
    slack_user_id TEXT NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, slack_user_id)
);

CREATE INDEX IF NOT EXISTS idx_slack_user_mappings_lookup
    ON slack_user_mappings(team_id, slack_user_id);
