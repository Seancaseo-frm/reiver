-- Pilot program: require manual approval before users can use the platform.
-- New users default to is_approved = false; platform admins toggle it.
-- To remove later: change DEFAULT to TRUE and drop the check in authenticate_request.

ALTER TABLE users ADD COLUMN IF NOT EXISTS is_approved BOOLEAN NOT NULL DEFAULT FALSE;

-- Approve all existing users so nothing breaks on deploy.
UPDATE users SET is_approved = true;
