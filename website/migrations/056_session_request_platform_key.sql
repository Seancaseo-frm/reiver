-- Track whether each session request used a platform-managed key or a BYOK key.
ALTER TABLE session_request_content
    ADD COLUMN IF NOT EXISTS is_platform_key BOOLEAN NOT NULL DEFAULT FALSE;
