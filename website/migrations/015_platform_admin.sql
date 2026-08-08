ALTER TABLE users ADD COLUMN IF NOT EXISTS is_platform_admin BOOLEAN NOT NULL DEFAULT FALSE;
-- Mark the first seeded user as platform admin.
-- In production, set this manually: UPDATE users SET is_platform_admin = true WHERE email = 'your-admin@example.com';
UPDATE users SET is_platform_admin = true WHERE email = 'dev@example.com';
