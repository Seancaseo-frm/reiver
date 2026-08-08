-- Add domain column to organizations for email-domain-based org matching
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS domain VARCHAR(255);
CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_domain ON organizations (domain) WHERE domain IS NOT NULL;

-- Backfill existing orgs with domain from the owner's email (skip public domains).
-- When multiple orgs share the same owner domain, only the oldest org gets the domain
-- to avoid violating the unique constraint.
UPDATE organizations o
SET domain = sub.d
FROM (
  SELECT DISTINCT ON (split_part(u.email, '@', 2))
         o2.id AS org_id,
         split_part(u.email, '@', 2) AS d
  FROM organizations o2
  JOIN memberships m ON m.organization_id = o2.id
  JOIN users u ON u.id = m.user_id
  WHERE m.role = 'owner'
    AND m.status = 'active'
    AND o2.domain IS NULL
    AND split_part(u.email, '@', 2) NOT IN (
      'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'live.com',
      'yahoo.com', 'yahoo.co.uk', 'yahoo.co.jp', 'ymail.com',
      'aol.com', 'icloud.com', 'me.com', 'mac.com',
      'protonmail.com', 'proton.me', 'pm.me',
      'mail.com', 'zoho.com', 'yandex.com', 'yandex.ru',
      'gmx.com', 'gmx.net', 'fastmail.com', 'tutanota.com', 'tuta.com',
      'qq.com', '163.com', '126.com', 'sina.com',
      'msn.com', 'att.net', 'comcast.net', 'verizon.net',
      'hey.com', 'duck.com', 'mailbox.org'
    )
  ORDER BY split_part(u.email, '@', 2), o2.created_at ASC
) sub
WHERE o.id = sub.org_id;

-- Organization invitations table
CREATE TABLE IF NOT EXISTS organization_invitations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email VARCHAR(255),
    role VARCHAR(50) NOT NULL DEFAULT 'member',
    invite_token VARCHAR(255) NOT NULL UNIQUE,
    invited_by UUID NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_org_invitations_org_email ON organization_invitations (organization_id, email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_org_invitations_token ON organization_invitations (invite_token);
CREATE INDEX IF NOT EXISTS idx_org_invitations_email ON organization_invitations (email) WHERE email IS NOT NULL AND accepted_at IS NULL;
