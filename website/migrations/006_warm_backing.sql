ALTER TABLE warehouse_sources
    ADD COLUMN backs_source_id UUID REFERENCES warehouse_sources(id) ON DELETE SET NULL;

CREATE INDEX idx_warehouse_sources_backs_source_id
    ON warehouse_sources(backs_source_id) WHERE backs_source_id IS NOT NULL;
