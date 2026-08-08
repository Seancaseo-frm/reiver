-- Add last_error column to surface schedule validation failures and refresh errors
-- to users through the API, instead of only logging them server-side.

ALTER TABLE warehouse_derived_tables
    ADD COLUMN IF NOT EXISTS last_error TEXT;
