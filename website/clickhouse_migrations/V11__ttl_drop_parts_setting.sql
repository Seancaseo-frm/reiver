-- Enable ttl_only_drop_parts so TTL can drop entire parts even when they
-- contain rows that haven't expired yet (the part's min_time/max_time bounds
-- are used). Without this, ClickHouse tries row-level TTL filtering which
-- fails for computed TTL columns (epoch-zero min/max in part metadata).
ALTER TABLE reiver.samples_v1_local MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE reiver.samples_v1_agg_5m_local MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE reiver.samples_v1_agg_30m_local MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE reiver.time_series_v1_local MODIFY SETTING ttl_only_drop_parts = 1;

-- Reduce the minimum interval between TTL merge passes from default (4h)
-- to 1 hour, ensuring expired data is cleaned up more promptly.
ALTER TABLE reiver.samples_v1_local MODIFY SETTING merge_with_ttl_timeout = 3600;
ALTER TABLE reiver.samples_v1_agg_5m_local MODIFY SETTING merge_with_ttl_timeout = 3600;
ALTER TABLE reiver.samples_v1_agg_30m_local MODIFY SETTING merge_with_ttl_timeout = 3600;
ALTER TABLE reiver.time_series_v1_local MODIFY SETTING merge_with_ttl_timeout = 3600;
