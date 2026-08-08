-- ============================================================================
-- Blockchain global sources and reference-source support
--
-- Adds infrastructure for shared blockchain data that is synced once globally
-- and referenced by individual projects without data duplication.
-- ============================================================================

-- Global blockchain sources (one row per chain, synced by a background daemon)
CREATE TABLE IF NOT EXISTS blockchain_global_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Chain identifier: 'bitcoin', 'ethereum', 'solana', 'polygon', etc.
    chain TEXT NOT NULL UNIQUE,

    -- RPC connection config (JSONB — rpc_url, credentials, etc.)
    -- NOTE: Credentials should be stored via the application's secret manager.
    -- This column holds non-sensitive config; sensitive fields (rpc_password)
    -- should be encrypted at the application layer before insertion.
    node_config JSONB NOT NULL,

    -- R2 object-key prefix where Parquet files are stored
    -- e.g. 'global/bitcoin'
    r2_prefix TEXT NOT NULL,

    -- Sync progress
    last_synced_height BIGINT NOT NULL DEFAULT 0,
    last_synced_hash TEXT,

    -- Last N block hashes for reorg detection.
    -- Format: {"<height_string>": "<hash>", ...}
    tip_hashes JSONB NOT NULL DEFAULT '{}',

    -- How many blocks from the tip are considered mutable (default 6 for Bitcoin).
    confirmation_depth INT NOT NULL DEFAULT 6
        CHECK (confirmation_depth >= 1),

    -- Daemon poll interval (same format as warehouse_sources.sync_interval).
    sync_interval TEXT NOT NULL DEFAULT '1m'
        CHECK (sync_interval IN ('10s', '30s', '1m', '5m', '15m', '30m', '1h')),

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  blockchain_global_sources IS 'Shared blockchain data sources synced once globally';
COMMENT ON COLUMN blockchain_global_sources.chain IS 'Lowercase chain name (bitcoin, ethereum, …)';
COMMENT ON COLUMN blockchain_global_sources.tip_hashes IS 'Recent block-height→hash map for reorg detection';
COMMENT ON COLUMN blockchain_global_sources.confirmation_depth IS 'Blocks from tip considered mutable (6 for Bitcoin)';

-- Add a nullable FK on warehouse_sources so per-project sources can
-- reference a global blockchain source.  When this is NOT NULL the source
-- is a lightweight reference — queries resolve via the global r2_prefix.
ALTER TABLE warehouse_sources
    ADD COLUMN IF NOT EXISTS global_source_id UUID REFERENCES blockchain_global_sources(id);

CREATE INDEX IF NOT EXISTS idx_warehouse_sources_global
    ON warehouse_sources(global_source_id) WHERE global_source_id IS NOT NULL;
