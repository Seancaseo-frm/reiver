-- Store R2 Parquet file keys directly in the derived table row to avoid
-- expensive R2 list_objects calls on every refresh and delete operation.

ALTER TABLE warehouse_derived_tables
    ADD COLUMN IF NOT EXISTS file_keys TEXT[] DEFAULT '{}';
