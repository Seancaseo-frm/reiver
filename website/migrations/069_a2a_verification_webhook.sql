-- Org-level verification webhook for cross-org A2A access.
-- When set, Herd POSTs the requester's owner email to this URL
-- so the target org can programmatically approve/deny access.

ALTER TABLE organizations ADD COLUMN IF NOT EXISTS verification_url TEXT;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS webhook_secret TEXT;
