-- Derived tables: tables created from queries (CTAS / materialized views)
--
-- Each derived table stores a SQL query definition and refresh metadata.
-- It also references a warehouse_sources row (source_type = 'derived')
-- and warehouse_tables row for query discovery via the rewriter.

CREATE TABLE IF NOT EXISTS warehouse_derived_tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    sql TEXT NOT NULL,
    description TEXT,
    -- 'full' = drop and rebuild; 'incremental' = append new rows
    refresh_mode TEXT NOT NULL DEFAULT 'full',
    -- Cron expression for scheduled refresh, NULL = manual only
    schedule TEXT,
    last_refreshed_at TIMESTAMPTZ,
    last_refresh_duration_ms BIGINT,
    last_refresh_rows BIGINT,
    row_count BIGINT DEFAULT 0,
    size_bytes BIGINT DEFAULT 0,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

CREATE INDEX IF NOT EXISTS idx_derived_tables_project
    ON warehouse_derived_tables (project_id);

CREATE INDEX IF NOT EXISTS idx_derived_tables_source
    ON warehouse_derived_tables (source_id);
