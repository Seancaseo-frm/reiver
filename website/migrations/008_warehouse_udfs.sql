-- UDF (User Defined Function) metadata tables.
--
-- warehouse_udfs stores compiled Go-to-Wasm UDFs with their source code,
-- compiled Wasm bytes, and manifest (input/output schemas).
-- warehouse_udf_runs tracks execution history for data movement jobs.

CREATE TABLE IF NOT EXISTS warehouse_udfs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    source_code TEXT NOT NULL,
    wasm_bytes BYTEA NOT NULL,
    manifest JSONB NOT NULL,
    execution_mode TEXT NOT NULL DEFAULT 'sql_function',
    schedule TEXT,
    fuel_limit BIGINT NOT NULL DEFAULT 10000000,
    timeout_secs INT NOT NULL DEFAULT 300,
    job_config JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

CREATE INDEX IF NOT EXISTS idx_warehouse_udfs_project
    ON warehouse_udfs (project_id);

CREATE TABLE IF NOT EXISTS warehouse_udf_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    udf_id UUID NOT NULL REFERENCES warehouse_udfs(id) ON DELETE CASCADE,
    project_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'submitted',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    fuel_consumed BIGINT,
    rows_read BIGINT,
    rows_written BIGINT,
    error_message TEXT,
    logs JSONB,
    trigger TEXT NOT NULL,
    job_config JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_warehouse_udf_runs_udf
    ON warehouse_udf_runs (udf_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_udf_runs_project
    ON warehouse_udf_runs (project_id);
