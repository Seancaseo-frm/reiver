-- Seed initial dev user for local development.
-- Use when ALLOW_SIGNUP=false and you need a test account.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO users (email, password_hash)
VALUES
  ('dev@example.com', crypt('change-me-in-production', gen_salt('bf', 12)));
