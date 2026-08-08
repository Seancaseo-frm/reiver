-- Maps a Slack channel to a specific Reiver project.
-- Used when a Slack workspace has integrations with multiple projects
-- so Moodeng knows which project context to use in each channel.
CREATE TABLE IF NOT EXISTS slack_channel_project_mappings (
    team_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    set_by_slack_user TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, channel_id)
);
