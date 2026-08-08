-- Add short alphanumeric slug to projects for human-friendly URLs.
-- Slugs are 8 characters, lowercase, unique.

ALTER TABLE projects ADD COLUMN slug VARCHAR(16);

-- Backfill existing projects with slug derived from their UUID (first 8 hex chars).
UPDATE projects SET slug = lower(left(replace(id::text, '-', ''), 8))
WHERE slug IS NULL;

-- Ensure uniqueness and make non-nullable.
ALTER TABLE projects ALTER COLUMN slug SET NOT NULL;
ALTER TABLE projects ADD CONSTRAINT projects_slug_unique UNIQUE (slug);

CREATE INDEX idx_projects_slug ON projects(slug);
