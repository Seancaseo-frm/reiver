-- Hash API keys for secure storage. Keys are looked up by SHA-256 hash
-- instead of plaintext. The `key` column is kept for encrypted storage
-- (AES-256-GCM at the application layer).

-- Add key_hash column for hash-based lookups
ALTER TABLE project_keys ADD COLUMN IF NOT EXISTS key_hash VARCHAR(64);

-- Backfill key_hash from existing plaintext keys using PostgreSQL built-in sha256
UPDATE project_keys SET key_hash = encode(sha256(key::bytea), 'hex') WHERE key_hash IS NULL;

-- Make key_hash NOT NULL after backfill
ALTER TABLE project_keys ALTER COLUMN key_hash SET NOT NULL;

-- Create unique index on key_hash (replaces plaintext key lookups)
CREATE UNIQUE INDEX IF NOT EXISTS idx_project_keys_key_hash ON project_keys(key_hash);

-- Drop the old unique constraint and index on plaintext key
DROP INDEX IF EXISTS idx_project_keys_key;
ALTER TABLE project_keys DROP CONSTRAINT IF EXISTS project_keys_key_key;

-- The `key` column is kept — new keys will be stored encrypted (AES-256-GCM).
-- A one-time application-level migration encrypts existing plaintext values.
