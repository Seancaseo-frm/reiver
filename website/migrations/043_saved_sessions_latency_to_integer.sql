ALTER TABLE saved_sessions ALTER COLUMN avg_latency_ms TYPE INTEGER USING avg_latency_ms::integer;
