ALTER TABLE a2a_access_grants
    ADD COLUMN requesting_project_id UUID;
ALTER TABLE a2a_access_grants
    ADD COLUMN requested_by UUID;

-- Backfill existing rows: use the first project in the granted org, and the org owner
UPDATE a2a_access_grants g
SET requesting_project_id = (
    SELECT p.id FROM projects p WHERE p.organization_id = g.granted_org_id LIMIT 1
)
WHERE g.requesting_project_id IS NULL;

UPDATE a2a_access_grants g
SET requested_by = (
    SELECT m.user_id FROM memberships m
    WHERE m.organization_id = g.granted_org_id AND m.role = 'owner' AND m.status = 'active'
    LIMIT 1
)
WHERE g.requested_by IS NULL;

ALTER TABLE a2a_access_grants
    ALTER COLUMN requesting_project_id SET NOT NULL;
ALTER TABLE a2a_access_grants
    ALTER COLUMN requested_by SET NOT NULL;
