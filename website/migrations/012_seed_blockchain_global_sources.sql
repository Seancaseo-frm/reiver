-- Seed global blockchain sources so users can enable them from the UI.
-- The BlockchainSyncDaemon reads from this table and keeps data up to date.

INSERT INTO blockchain_global_sources (chain, node_config, r2_prefix, confirmation_depth, sync_interval, enabled)
VALUES
  (
    'bitcoin',
    '{"rpc_url": "https://bitcoin-mainnet.public.blastapi.io", "network": "mainnet"}'::jsonb,
    'global/bitcoin',
    6,
    '30s',
    true
  ),
  (
    'ethereum',
    '{"rpc_url": "https://ethereum-rpc.publicnode.com", "batch_size": 50, "max_retries": 3}'::jsonb,
    'global/ethereum',
    12,
    '30s',
    true
  )
ON CONFLICT (chain) DO NOTHING;
