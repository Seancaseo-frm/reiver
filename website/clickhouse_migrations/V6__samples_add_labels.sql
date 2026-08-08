-- Denormalize labels into samples_v1 so the PromQL evaluator can read them
-- directly without JOINing time_series_v1. Existing rows return '' (empty).
-- No ON CLUSTER needed — the Replicated database engine replicates DDL automatically.

ALTER TABLE reiver.samples_v1_local
  ADD COLUMN IF NOT EXISTS `labels` String DEFAULT '';

ALTER TABLE reiver.samples_v1
  ADD COLUMN IF NOT EXISTS `labels` String DEFAULT '';
