-- Add profile-level attributes (from Profile.attribute_indices) and comments
-- (from Profile.comment_strindices) to the profiles tables.
ALTER TABLE reiver.profiles_local
    ADD COLUMN IF NOT EXISTS `attributes` Map(LowCardinality(String), String) DEFAULT map();

ALTER TABLE reiver.profiles_local
    ADD COLUMN IF NOT EXISTS `comments` Array(String) DEFAULT [];

ALTER TABLE reiver.profiles
    ADD COLUMN IF NOT EXISTS `attributes` Map(LowCardinality(String), String) DEFAULT map();

ALTER TABLE reiver.profiles
    ADD COLUMN IF NOT EXISTS `comments` Array(String) DEFAULT [];

-- Add per-sample labels (from Sample.attribute_indices) to the profile_samples tables.
ALTER TABLE reiver.profile_samples_local
    ADD COLUMN IF NOT EXISTS `labels` Map(LowCardinality(String), String) DEFAULT map();

ALTER TABLE reiver.profile_samples
    ADD COLUMN IF NOT EXISTS `labels` Map(LowCardinality(String), String) DEFAULT map();
