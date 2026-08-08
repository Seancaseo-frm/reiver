-- Pipeline (DAG) definitions for transformation workflows.
--
-- A pipeline is a directed acyclic graph of source, transform, and sink nodes.
-- Nodes are connected by edges that define data flow.

CREATE TABLE IF NOT EXISTS warehouse_pipelines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    schedule TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

CREATE INDEX IF NOT EXISTS idx_warehouse_pipelines_project
    ON warehouse_pipelines (project_id);

CREATE TABLE IF NOT EXISTS warehouse_pipeline_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES warehouse_pipelines(id) ON DELETE CASCADE,
    node_type TEXT NOT NULL,
    label TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    position_x REAL NOT NULL DEFAULT 0,
    position_y REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_warehouse_pipeline_nodes_pipeline
    ON warehouse_pipeline_nodes (pipeline_id);

CREATE TABLE IF NOT EXISTS warehouse_pipeline_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES warehouse_pipelines(id) ON DELETE CASCADE,
    from_node_id UUID NOT NULL REFERENCES warehouse_pipeline_nodes(id) ON DELETE CASCADE,
    to_node_id UUID NOT NULL REFERENCES warehouse_pipeline_nodes(id) ON DELETE CASCADE,
    UNIQUE(from_node_id, to_node_id)
);

CREATE INDEX IF NOT EXISTS idx_warehouse_pipeline_edges_pipeline
    ON warehouse_pipeline_edges (pipeline_id);

CREATE TABLE IF NOT EXISTS warehouse_pipeline_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL REFERENCES warehouse_pipelines(id) ON DELETE CASCADE,
    project_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    trigger TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    step_results JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_warehouse_pipeline_runs_pipeline
    ON warehouse_pipeline_runs (pipeline_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_pipeline_runs_project
    ON warehouse_pipeline_runs (project_id);

CREATE TABLE IF NOT EXISTS warehouse_pipeline_cursors (
    pipeline_id UUID NOT NULL REFERENCES warehouse_pipelines(id) ON DELETE CASCADE,
    node_id UUID NOT NULL REFERENCES warehouse_pipeline_nodes(id) ON DELETE CASCADE,
    last_value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, node_id)
);
