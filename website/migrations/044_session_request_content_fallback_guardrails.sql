-- Add fallback and guardrail tracking columns to session_request_content
-- so that session replay can show when fallbacks/guardrails were triggered.

ALTER TABLE session_request_content
    ADD COLUMN IF NOT EXISTS fallback_used BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS original_model TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS guardrail_violations TEXT[] NOT NULL DEFAULT '{}';

-- Also add aggregate counts to saved_sessions for the sessions list page
ALTER TABLE saved_sessions
    ADD COLUMN IF NOT EXISTS fallback_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS guardrail_count INTEGER NOT NULL DEFAULT 0;
