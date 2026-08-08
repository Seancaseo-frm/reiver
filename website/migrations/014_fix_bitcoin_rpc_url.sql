-- Switch Bitcoin global source to PublicNode's free RPC endpoint.
-- The original BlastAPI endpoint returns HTTP 403.
UPDATE blockchain_global_sources
SET node_config = jsonb_set(node_config, '{rpc_url}', '"https://bitcoin-rpc.publicnode.com"')
WHERE chain = 'bitcoin';
