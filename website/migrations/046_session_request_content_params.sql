-- Store request parameters per session request so replay can load original values.
ALTER TABLE session_request_content
    ADD COLUMN IF NOT EXISTS temperature REAL,
    ADD COLUMN IF NOT EXISTS top_p REAL,
    ADD COLUMN IF NOT EXISTS max_tokens INTEGER,
    ADD COLUMN IF NOT EXISTS frequency_penalty REAL,
    ADD COLUMN IF NOT EXISTS presence_penalty REAL;
