-- Default: manual approval required for new self-serve signups (matches pre-migration behavior).
INSERT INTO platform_settings (key, value, updated_at)
VALUES ('require_signup_approval', 'true', NOW())
ON CONFLICT (key) DO NOTHING;
