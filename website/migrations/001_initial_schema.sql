-- Website (auth/identity/platform) PostgreSQL Schema

-- ============================================================================
-- Core Identity & Organization Tables
-- ============================================================================

-- Organizations (top-level tenant boundary)
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_organizations_name ON organizations(name);

-- Users (global identity - can belong to multiple organizations)
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255), -- nullable if SSO-only
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);

-- Memberships (many-to-many: User ↔ Organization with role)
CREATE TABLE memberships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL, -- 'owner', 'admin', 'member', 'viewer'
    status VARCHAR(50) DEFAULT 'active', -- 'active', 'invited', 'suspended'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, organization_id)
);

CREATE INDEX idx_memberships_user_id ON memberships(user_id);
CREATE INDEX idx_memberships_organization_id ON memberships(organization_id);
CREATE INDEX idx_memberships_user_org ON memberships(user_id, organization_id);

-- ============================================================================
-- Projects (belong to Organization, not User)
-- ============================================================================

CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    created_by UUID REFERENCES users(id), -- who created it (audit)
    github_repo_url VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settings JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_projects_organization_id ON projects(organization_id);
CREATE INDEX idx_projects_created_by ON projects(created_by);
CREATE INDEX idx_projects_github_repo ON projects(github_repo_url) WHERE github_repo_url IS NOT NULL;

-- Project API Keys
CREATE TABLE project_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key VARCHAR(255) UNIQUE NOT NULL,
    rate_limit INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_project_keys_key ON project_keys(key);
CREATE INDEX idx_project_keys_project_id ON project_keys(project_id);

-- ============================================================================
-- SSO & Authentication Tables
-- ============================================================================

-- SSO provider configurations (per organization)
CREATE TABLE sso_configurations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    
    -- Provider type: 'okta', 'auth0', 'entra_id', 'onelogin', 'ping', 'keycloak'
    provider VARCHAR(50) NOT NULL,
    
    -- Display name for this SSO configuration
    name VARCHAR(255) NOT NULL,
    
    -- Domain name for multi-domain per org
    domain_name VARCHAR(255),
    
    -- SSO type to distinguish between OIDC and SAML
    sso_type VARCHAR(20) NOT NULL DEFAULT 'oidc', -- 'oidc' or 'saml'
    
    -- OIDC Configuration
    issuer_url TEXT, -- nullable for SAML
    issuer_alias TEXT, -- Alternative issuer URL for OIDC discovery (for Azure/Oracle quirks)
    client_id VARCHAR(255), -- nullable for SAML
    client_secret_encrypted TEXT, -- nullable for SAML
    
    -- SAML Configuration fields
    saml_entity_id TEXT, -- IdP Entity ID
    saml_sso_url TEXT, -- IdP SSO URL (HTTP-POST binding)
    saml_certificate TEXT, -- IdP X.509 certificate
    saml_sign_requests BOOLEAN DEFAULT true, -- Whether to sign AuthnRequests
    
    -- SAML SP Signing Certificate (optional - for AuthnRequest signing)
    sp_certificate TEXT, -- PEM-encoded X.509 certificate for SP AuthnRequest signing
    sp_private_key_encrypted TEXT, -- Encrypted PEM-encoded private key for SP signing
    
    -- SAML Single Logout (SLO) URL (optional)
    saml_slo_url TEXT, -- IdP Single Logout URL (HTTP-Redirect binding) - optional for SLO support
    
    -- Optional: Okta-specific settings
    okta_domain VARCHAR(255),
    okta_api_token_encrypted TEXT, -- For user sync via Okta Admin API
    
    -- Scopes to request (default: openid profile email)
    scopes TEXT[] NOT NULL DEFAULT ARRAY['openid', 'profile', 'email'],
    
    -- User provisioning settings
    auto_create_users BOOLEAN NOT NULL DEFAULT true,
    default_role VARCHAR(50) NOT NULL DEFAULT 'member',
    
    -- Restrict to specific email domains (empty = allow all)
    allowed_email_domains TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    
    -- SCIM settings
    scim_enabled BOOLEAN NOT NULL DEFAULT false,
    scim_bearer_token_hash VARCHAR(255),
    
    -- Settings
    enabled BOOLEAN NOT NULL DEFAULT true,
    
    -- Security settings
    secrets_encrypted BOOLEAN NOT NULL DEFAULT true,
    last_rotated_at TIMESTAMPTZ,
    rotation_required BOOLEAN NOT NULL DEFAULT false,
    require_mfa BOOLEAN NOT NULL DEFAULT false,
    mfa_methods JSONB DEFAULT '["totp", "webauthn"]',
    
    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sso_configurations_organization_id ON sso_configurations(organization_id);

-- Partial unique constraints: one config per domain per org, or one per provider per org (for non-domain configs)
CREATE UNIQUE INDEX idx_sso_configurations_org_domain ON sso_configurations(organization_id, domain_name) 
    WHERE domain_name IS NOT NULL;
CREATE UNIQUE INDEX idx_sso_configurations_org_provider ON sso_configurations(organization_id, provider) 
    WHERE domain_name IS NULL;
CREATE INDEX idx_sso_configurations_domain ON sso_configurations(domain_name, enabled);
CREATE INDEX idx_sso_configurations_provider ON sso_configurations(provider, enabled);

-- SSO user mappings (links SSO identity to local user)
CREATE TABLE sso_user_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sso_config_id UUID NOT NULL REFERENCES sso_configurations(id) ON DELETE CASCADE,
    
    -- External identity
    external_id VARCHAR(255) NOT NULL, -- Okta user ID (sub claim)
    external_email VARCHAR(255),
    
    -- SCIM fields
    scim_id VARCHAR(255),
    provisioned_via_scim BOOLEAN NOT NULL DEFAULT false,
    scim_active BOOLEAN NOT NULL DEFAULT true,
    
    -- Metadata
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (sso_config_id, external_id),
    UNIQUE (user_id, sso_config_id)
);

CREATE INDEX idx_sso_user_mappings_user ON sso_user_mappings(user_id);
CREATE INDEX idx_sso_user_mappings_external ON sso_user_mappings(sso_config_id, external_id);
CREATE INDEX idx_sso_user_mappings_scim_id ON sso_user_mappings(sso_config_id, scim_id);

-- SP (Service Provider) certificates for SAML signing
CREATE TABLE sp_certificates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    
    -- Certificate data (PEM format)
    certificate_pem TEXT NOT NULL,
    private_key_encrypted TEXT NOT NULL,  -- Encrypted using SecretEncryptor
    
    -- Certificate metadata
    fingerprint VARCHAR(64) NOT NULL,     -- SHA-256 fingerprint
    subject_dn TEXT,                       -- Certificate subject
    issuer_dn TEXT,                        -- Certificate issuer (self-signed = same as subject)
    serial_number VARCHAR(64),
    
    -- Validity
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL,
    
    -- Status
    is_active BOOLEAN NOT NULL DEFAULT true,
    
    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),
    revoked_at TIMESTAMPTZ,
    revoked_by UUID REFERENCES users(id),
    revocation_reason VARCHAR(255)
);

CREATE INDEX idx_sp_certificates_org ON sp_certificates(organization_id, is_active);
CREATE INDEX idx_sp_certificates_fingerprint ON sp_certificates(fingerprint);

-- SSO sessions for session tracking and SLO
CREATE TABLE sso_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sso_config_id UUID NOT NULL REFERENCES sso_configurations(id) ON DELETE CASCADE,
    
    -- Session token
    session_token_hash VARCHAR(64) NOT NULL,  -- SHA-256 hash of session token
    
    -- IdP session (for SLO)
    idp_session_id VARCHAR(255),              -- Session index from IdP
    
    -- Client info
    ip_address INET,
    user_agent TEXT,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Revocation
    revoked_at TIMESTAMPTZ,
    revocation_reason VARCHAR(50)             -- 'user_logout', 'admin_revoke', 'slo', 'expired'
);

CREATE INDEX idx_sso_sessions_user ON sso_sessions(user_id, revoked_at);
CREATE INDEX idx_sso_sessions_token ON sso_sessions(session_token_hash);
CREATE INDEX idx_sso_sessions_idp ON sso_sessions(sso_config_id, idp_session_id);

-- JIT Provisioning rules
CREATE TABLE provisioning_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    sso_config_id UUID REFERENCES sso_configurations(id) ON DELETE CASCADE,
    
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Priority (lower = higher priority)
    priority INTEGER NOT NULL DEFAULT 100,
    enabled BOOLEAN NOT NULL DEFAULT true,
    
    -- Rule condition (JSON)
    condition JSONB NOT NULL DEFAULT '{"type": "always"}',
    
    -- Actions to take when condition matches (JSON array)
    actions JSONB NOT NULL DEFAULT '[]',
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_provisioning_rules_org ON provisioning_rules(organization_id, enabled);
CREATE INDEX idx_provisioning_rules_sso ON provisioning_rules(sso_config_id);

-- ============================================================================
-- MFA & WebAuthn Tables
-- ============================================================================

-- MFA enrollments (tracks which MFA methods a user has enabled)
CREATE TABLE mfa_enrollments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Method type: 'totp', 'webauthn'
    method VARCHAR(20) NOT NULL,
    
    -- TOTP-specific: encrypted secret
    secret_encrypted TEXT,
    
    -- Display name
    name VARCHAR(255),
    
    -- Is this the primary MFA method?
    is_primary BOOLEAN NOT NULL DEFAULT false,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE INDEX idx_mfa_enrollments_user ON mfa_enrollments(user_id);
CREATE UNIQUE INDEX idx_mfa_enrollments_user_method ON mfa_enrollments(user_id, method) WHERE method = 'totp';

-- MFA recovery codes
CREATE TABLE mfa_recovery_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash VARCHAR(64) NOT NULL,  -- SHA-256 hash
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    used_at TIMESTAMPTZ
);

CREATE INDEX idx_mfa_recovery_codes_user ON mfa_recovery_codes(user_id, used_at);
CREATE UNIQUE INDEX idx_mfa_recovery_codes_hash ON mfa_recovery_codes(user_id, code_hash);

-- WebAuthn credentials
CREATE TABLE webauthn_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- Credential data
    credential_id BYTEA NOT NULL,
    public_key BYTEA NOT NULL,
    counter BIGINT NOT NULL DEFAULT 0,
    
    -- Metadata
    name VARCHAR(255) NOT NULL DEFAULT 'Security Key',
    aaguid BYTEA,  -- Authenticator identifier
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE INDEX idx_webauthn_credentials_user ON webauthn_credentials(user_id);
CREATE UNIQUE INDEX idx_webauthn_credentials_id ON webauthn_credentials(credential_id);

-- ============================================================================
-- Audit Events
-- ============================================================================

-- Audit events for security logging
CREATE TABLE audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Context
    organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    actor_id UUID REFERENCES users(id) ON DELETE SET NULL,  -- Who performed the action
    
    -- Request info
    ip_address INET,
    user_agent TEXT,
    
    -- Resource affected
    resource_type VARCHAR(100),
    resource_id UUID,
    
    -- Event details (JSON)
    details JSONB,
    
    -- Outcome
    success BOOLEAN NOT NULL DEFAULT true,
    error_message TEXT
);

CREATE INDEX idx_audit_events_org ON audit_events(organization_id, timestamp DESC);
CREATE INDEX idx_audit_events_user ON audit_events(user_id, timestamp DESC);
CREATE INDEX idx_audit_events_type ON audit_events(event_type, timestamp DESC);
CREATE INDEX idx_audit_events_resource ON audit_events(resource_type, resource_id);
CREATE INDEX idx_audit_events_timestamp ON audit_events(timestamp DESC);

-- ============================================================================
-- SCIM Group Mappings
-- ============================================================================

-- SCIM group mappings (maps external IdP groups to Reiver roles)
CREATE TABLE scim_group_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sso_config_id UUID NOT NULL REFERENCES sso_configurations(id) ON DELETE CASCADE,
    
    -- External group info from IdP
    external_group_id VARCHAR(255) NOT NULL,
    external_group_name VARCHAR(255) NOT NULL,
    
    -- Reiver role to assign
    reiver_role VARCHAR(50) NOT NULL, -- 'admin', 'member', 'viewer'
    
    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (sso_config_id, external_group_id)
);

CREATE INDEX idx_scim_group_mappings_sso ON scim_group_mappings(sso_config_id);

-- ============================================================================
-- Dashboards & Visualization Tables
-- ============================================================================

CREATE TABLE dashboards (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_default BOOLEAN NOT NULL DEFAULT false,
    layout_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    refresh_interval INTEGER DEFAULT 30,
    time_range VARCHAR(50) DEFAULT '1h',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_dashboards_project_id ON dashboards(project_id);
CREATE INDEX idx_dashboards_user_id ON dashboards(user_id);
CREATE INDEX idx_dashboards_is_default ON dashboards(project_id, is_default);

-- Dashboard Tabs (group widgets into tabs within a dashboard)
CREATE TABLE dashboard_tabs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    display_order INTEGER NOT NULL DEFAULT 0,
    icon VARCHAR(50), -- optional icon identifier
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dashboard_tabs_dashboard ON dashboard_tabs(dashboard_id, display_order);

CREATE TABLE dashboard_widgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dashboard_id UUID NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    tab_id UUID REFERENCES dashboard_tabs(id) ON DELETE CASCADE, -- nullable for dashboards without tabs
    widget_type VARCHAR(50) NOT NULL, -- 'timeseries', 'histogram', 'bar', 'table', 'stat', 'heatmap'
    widget_config JSONB NOT NULL DEFAULT '{}'::jsonb, -- contains query config, field overrides, display options
    position_x INTEGER NOT NULL DEFAULT 0,
    position_y INTEGER NOT NULL DEFAULT 0,
    width INTEGER NOT NULL DEFAULT 4,
    height INTEGER NOT NULL DEFAULT 3,
    title VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dashboard_widgets_dashboard_id ON dashboard_widgets(dashboard_id);
CREATE INDEX idx_dashboard_widgets_tab ON dashboard_widgets(tab_id);
CREATE INDEX idx_dashboard_widgets_position ON dashboard_widgets(dashboard_id, position_y, position_x);

-- Dashboard Templates (pre-built dashboards users can choose from)
-- Templates use tabs structure: { "tabs": [{ "name": "...", "widgets": [...] }] }
-- Widgets define queries using OTel semantic conventions
CREATE TABLE dashboard_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    category VARCHAR(100) NOT NULL DEFAULT 'general', -- 'services', 'infrastructure', 'logs', 'general'
    thumbnail_url TEXT,
    template_config JSONB NOT NULL DEFAULT '{}'::jsonb, -- { "tabs": [...], "variables": [...] }
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    is_featured BOOLEAN DEFAULT false,
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dashboard_templates_category ON dashboard_templates(category);
CREATE INDEX idx_dashboard_templates_featured ON dashboard_templates(is_featured, display_order);
CREATE INDEX idx_dashboard_templates_tags ON dashboard_templates USING GIN(tags);

-- Custom Graphs Table
CREATE TABLE custom_graphs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    chart_type VARCHAR(50) NOT NULL DEFAULT 'line',
    graphql_query TEXT NOT NULL,
    query_variables JSONB DEFAULT '{}'::jsonb,
    chart_config JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_custom_graphs_project_id ON custom_graphs(project_id);
CREATE INDEX idx_custom_graphs_user_id ON custom_graphs(user_id);
CREATE INDEX idx_custom_graphs_project_user ON custom_graphs(project_id, user_id);

-- ============================================================================
-- Alert System Tables
-- ============================================================================

-- Alert Rules Table
CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    -- Rule definition
    name VARCHAR(255) NOT NULL,
    description TEXT,
    rule_type VARCHAR(50) NOT NULL DEFAULT 'threshold',
    
    -- Query configuration (JSON)
    query_config JSONB NOT NULL,
    
    -- Single threshold (simplified model)
    threshold FLOAT8 NOT NULL DEFAULT 0,
    threshold_type VARCHAR(10) NOT NULL DEFAULT 'above',  -- 'above' or 'below'
    
    -- Notification channels (array of channel UUIDs)
    notification_channels UUID[] NOT NULL DEFAULT '{}',
    
    -- Alert on absent data
    alert_on_absent BOOLEAN DEFAULT false,
    absent_for_seconds INTEGER DEFAULT 300,
    
    -- Evaluation settings
    eval_window_seconds INTEGER NOT NULL DEFAULT 300,
    eval_interval_seconds INTEGER NOT NULL DEFAULT 60,
    
    -- Labels and annotations
    labels JSONB DEFAULT '{}',
    annotations JSONB DEFAULT '{}',
    
    -- State
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_evaluated_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_rules_project ON alert_rules(project_id);
CREATE INDEX idx_alert_rules_enabled ON alert_rules(enabled, last_evaluated_at);
CREATE INDEX idx_alert_rules_eval_due ON alert_rules(enabled, last_evaluated_at, eval_interval_seconds) 
    WHERE enabled = true;

-- Active Alerts Table
CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    
    fingerprint VARCHAR(64) NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}',
    annotations JSONB DEFAULT '{}',
    
    -- Alert state: OK or ALERT (simplified from pending/firing/recovering/resolved)
    state VARCHAR(20) NOT NULL DEFAULT 'OK',
    value FLOAT8,
    
    -- Track when state last changed (for auto-reset after eval_window_seconds)
    state_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(rule_id, fingerprint)
);

CREATE INDEX idx_alerts_rule_state ON alerts(rule_id, state);
CREATE INDEX idx_alerts_state_alert ON alerts(state) WHERE state = 'ALERT';
CREATE INDEX idx_alerts_checked_at ON alerts(checked_at DESC);

-- Alert History Table
CREATE TABLE alert_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    
    state VARCHAR(20) NOT NULL,  -- OK or ALERT
    value FLOAT8,
    
    notification_sent BOOLEAN DEFAULT false,
    notification_error TEXT,
    
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_history_alert ON alert_history(alert_id, checked_at DESC);
CREATE INDEX idx_alert_history_rule_time ON alert_history(rule_id, checked_at DESC);
CREATE INDEX idx_alert_history_ttl ON alert_history(checked_at);  -- For cleanup of old records

-- Function to update updated_at timestamp for alert_rules
CREATE OR REPLACE FUNCTION update_alert_rules_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_alert_rules_updated_at
    BEFORE UPDATE ON alert_rules
    FOR EACH ROW
    EXECUTE FUNCTION update_alert_rules_updated_at();

-- ============================================================================
-- Sampling & Tracing Tables
-- ============================================================================

CREATE TABLE sampling_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL UNIQUE REFERENCES projects(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    decision_wait_seconds INTEGER NOT NULL DEFAULT 10,
    max_traces INTEGER NOT NULL DEFAULT 50000,
    config_jsonb JSONB NOT NULL DEFAULT '{
        "policies": [
            {
                "name": "error-traces",
                "priority": 1,
                "policy_type": "AlwaysSample",
                "sampling_percentage": 100.0,
                "filter": {
                    "filter_op": "OR",
                    "string_attributes": [
                        {
                            "key": "status",
                            "values": ["error"],
                            "enabled_regex_matching": false,
                            "invert_match": false
                        }
                    ],
                    "numeric_attributes": [
                        {
                            "key": "http.status_code",
                            "min_value": 400,
                            "max_value": 599
                        }
                    ]
                }
            },
            {
                "name": "slow-traces",
                "priority": 2,
                "policy_type": {
                    "Latency": {
                        "min_duration_ms": 5000
                    }
                },
                "sampling_percentage": 100.0,
                "filter": null
            },
            {
                "name": "default",
                "priority": 3,
                "policy_type": {
                    "Probabilistic": {
                        "hash_salt": null
                    }
                },
                "sampling_percentage": 10.0,
                "filter": null
            }
        ]
    }'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sampling_configs_project_id ON sampling_configs(project_id);
CREATE INDEX idx_sampling_configs_enabled ON sampling_configs(enabled);

-- ============================================================================
-- Notification Channels
-- ============================================================================

-- Unified Notification Channels
-- Supports: slack, pagerduty, teams, discord, webhook
CREATE TABLE notification_channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    
    -- Channel type: 'slack', 'pagerduty', 'teams', 'discord', 'webhook'
    channel_type VARCHAR(50) NOT NULL,
    
    -- Configuration stored as JSONB (type-specific fields)
    -- Slack: { "webhook_url": "https://hooks.slack.com/...", "channel": "#alerts" }
    -- PagerDuty: { "routing_key": "...", "service_id": "..." }
    -- Teams: { "webhook_url": "https://..." }
    -- Discord: { "webhook_url": "https://discord.com/api/webhooks/..." }
    -- Webhook: { "url": "https://...", "headers": {...}, "method": "POST" }
    config JSONB NOT NULL DEFAULT '{}',
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_notification_channels_project ON notification_channels(project_id, enabled);
CREATE INDEX idx_notification_channels_type ON notification_channels(channel_type, enabled);

-- ============================================================================
-- Monitoring & Feature Flag Tables
-- ============================================================================

-- Feature Flag Changes
CREATE TABLE feature_flag_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    flag_id VARCHAR(255) NOT NULL,
    flag_name VARCHAR(255),
    environment VARCHAR(50),
    change_type VARCHAR(50) NOT NULL,
    
    prev_value JSONB,
    new_value JSONB NOT NULL,
    
    changed_by JSONB,
    
    impacted_services TEXT[],
    
    metadata JSONB,
    
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_flag_changes_project_time ON feature_flag_changes(project_id, timestamp DESC);
CREATE INDEX idx_flag_changes_flag_time ON feature_flag_changes(flag_id, timestamp DESC);
CREATE INDEX idx_flag_changes_services ON feature_flag_changes USING GIN(impacted_services);
CREATE INDEX idx_flag_changes_env ON feature_flag_changes(project_id, environment, timestamp DESC);
CREATE INDEX idx_flag_changes_type ON feature_flag_changes(project_id, change_type, timestamp DESC);

-- Feature Flag Usage
CREATE TABLE feature_flag_usage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    flag_id VARCHAR(255) NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    environment VARCHAR(50),
    
    evaluation_count BIGINT NOT NULL DEFAULT 0,
    enabled_count BIGINT NOT NULL DEFAULT 0,
    disabled_count BIGINT NOT NULL DEFAULT 0,
    
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, flag_id, service_name, environment)
);

CREATE INDEX idx_flag_usage_project ON feature_flag_usage(project_id, last_seen DESC);
CREATE INDEX idx_flag_usage_flag ON feature_flag_usage(flag_id, last_seen DESC);
CREATE INDEX idx_flag_usage_service ON feature_flag_usage(project_id, service_name, last_seen DESC);

-- Health Check Configs
CREATE TABLE health_check_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    check_type VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    
    target_url TEXT,
    target_host VARCHAR(255),
    target_port INTEGER,
    
    http_method VARCHAR(10) DEFAULT 'GET',
    http_headers JSONB DEFAULT '{}',
    http_body TEXT,
    http_expected_status INTEGER[],
    http_expected_body TEXT,
    http_follow_redirects BOOLEAN DEFAULT true,
    http_timeout_ms INTEGER DEFAULT 10000,
    
    tcp_send_data TEXT,
    tcp_expect_data TEXT,
    
    ssl_check_expiry BOOLEAN DEFAULT true,
    ssl_expiry_warning_days INTEGER DEFAULT 30,
    ssl_check_chain BOOLEAN DEFAULT true,
    
    check_interval_seconds INTEGER NOT NULL DEFAULT 60,
    timeout_seconds INTEGER DEFAULT 30,
    
    locations TEXT[] DEFAULT ARRAY['us-east']::TEXT[],
    
    response_time_threshold_ms INTEGER,
    
    min_failing_locations INTEGER DEFAULT 1,
    alert_after_minutes INTEGER DEFAULT 0,
    
    failure_threshold INTEGER DEFAULT 3,
    success_threshold INTEGER DEFAULT 1,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_check_at TIMESTAMPTZ,
    last_status VARCHAR(20),
    consecutive_failures INTEGER DEFAULT 0,
    consecutive_successes INTEGER DEFAULT 0,
    
    location_statuses JSONB DEFAULT '{}',
    
    alert_triggered_at TIMESTAMPTZ,
    
    tags JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_health_checks_project ON health_check_configs(project_id);
CREATE INDEX idx_health_checks_enabled ON health_check_configs(enabled, last_check_at);
CREATE INDEX idx_health_checks_type ON health_check_configs(check_type);

-- Auth Event Integration Configs (for IdP event ingestion)
CREATE TABLE auth_event_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    provider VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    
    domain VARCHAR(255),
    tenant_id VARCHAR(255),
    environment_id VARCHAR(255),
    region VARCHAR(50),
    
    api_token_encrypted TEXT,
    client_id VARCHAR(255),
    client_secret_encrypted TEXT,
    
    poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
    
    event_types TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    last_poll_at TIMESTAMPTZ,
    last_event_id VARCHAR(255),
    last_event_timestamp TIMESTAMPTZ,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    error_message TEXT,
    consecutive_errors INTEGER DEFAULT 0,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, provider)
);

CREATE INDEX idx_auth_event_configs_enabled ON auth_event_integration_configs(enabled, last_poll_at);
CREATE INDEX idx_auth_event_configs_project ON auth_event_integration_configs(project_id);

-- ============================================================================
-- Maintenance Windows
-- Allows scheduling maintenance periods during which alerts and health checks are silenced
-- ============================================================================

CREATE TABLE maintenance_windows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,

    -- Basic info
    name VARCHAR(255) NOT NULL,
    description TEXT,

    -- Schedule type: 'one_time' or 'recurring'
    schedule_type VARCHAR(20) NOT NULL DEFAULT 'one_time',

    -- For one_time: absolute start and end times (stored in UTC)
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,

    -- For recurring schedules
    recurrence_type VARCHAR(20), -- 'daily', 'weekly', 'monthly'
    recurrence_days INTEGER[], -- weekly: 0=Sun, 1=Mon, ..., 6=Sat; monthly: day of month (1-31)
    recurrence_start_time TIME, -- Time of day (HH:MM) when maintenance starts
    recurrence_duration_minutes INTEGER, -- Duration in minutes
    recurrence_timezone VARCHAR(100) DEFAULT 'UTC', -- IANA timezone (e.g., 'America/New_York')
    recurrence_end_date DATE, -- Optional end date for recurring windows

    -- Status
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- Metadata
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_maintenance_windows_project ON maintenance_windows(project_id);
CREATE INDEX idx_maintenance_windows_enabled ON maintenance_windows(enabled);
CREATE INDEX idx_maintenance_windows_schedule ON maintenance_windows(schedule_type, start_time, end_time);

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_maintenance_windows_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_maintenance_windows_updated_at
    BEFORE UPDATE ON maintenance_windows
    FOR EACH ROW
    EXECUTE FUNCTION update_maintenance_windows_updated_at();

-- ============================================================================
-- LLM Monitoring Features
-- Dynamic pricing, evaluations, and session metadata for AI observability
-- ============================================================================

-- ============================================================================
-- LLM Pricing: Dynamic pricing fetched from external sources
-- ============================================================================

CREATE TABLE llm_pricing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider VARCHAR(100) NOT NULL,          -- 'openai', 'anthropic', 'google', etc.
    model VARCHAR(255) NOT NULL,             -- 'gpt-4o', 'claude-3-opus', etc.
    model_aliases JSONB DEFAULT '[]',        -- Alternative model names/IDs
    
    -- Pricing per million tokens (USD)
    input_cost_per_1m DECIMAL(12, 8) NOT NULL,
    output_cost_per_1m DECIMAL(12, 8) NOT NULL,
    cache_read_cost_per_1m DECIMAL(12, 8) DEFAULT 0,
    cache_write_cost_per_1m DECIMAL(12, 8) DEFAULT 0,
    
    -- Model capabilities
    context_length INT,
    max_output_tokens INT,
    supports_vision BOOLEAN DEFAULT FALSE,
    supports_function_calling BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    source VARCHAR(50) DEFAULT 'helicone',   -- 'helicone', 'openrouter', 'manual'
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(provider, model)
);

CREATE INDEX idx_llm_pricing_provider ON llm_pricing(provider);
CREATE INDEX idx_llm_pricing_model ON llm_pricing(model);
CREATE INDEX idx_llm_pricing_source ON llm_pricing(source);
CREATE INDEX idx_llm_pricing_last_updated ON llm_pricing(last_updated);

-- Sync history for pricing updates
CREATE TABLE llm_pricing_sync_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_type VARCHAR(50) NOT NULL,          -- 'startup', 'daily', 'manual'
    source VARCHAR(50) NOT NULL,             -- 'helicone', 'openrouter', 'all'
    models_added INT DEFAULT 0,
    models_updated INT DEFAULT 0,
    status VARCHAR(20) NOT NULL,             -- 'success', 'partial', 'failed'
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_llm_pricing_sync_log_started_at ON llm_pricing_sync_log(started_at DESC);

-- ============================================================================
-- Evaluation Scores: Quality metrics for LLM responses
-- ============================================================================

CREATE TABLE llm_evaluation_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    -- Reference to the request being evaluated
    request_id VARCHAR(255) NOT NULL,        -- ID from llm_requests in ClickHouse
    
    -- Score details
    score_name VARCHAR(100) NOT NULL,        -- 'relevance', 'accuracy', 'coherence', etc.
    score_value DECIMAL(5, 2) NOT NULL,      -- Normalized 0-100 or boolean (0/1)
    score_type VARCHAR(20) DEFAULT 'number', -- 'number', 'boolean', 'category'
    
    -- Optional explanation
    reason TEXT,
    
    -- Who/what created this score
    evaluator_type VARCHAR(50),              -- 'human', 'llm-as-judge', 'automated'
    evaluator_id VARCHAR(255),               -- User ID or evaluator model name
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_llm_eval_scores_project_id ON llm_evaluation_scores(project_id);
CREATE INDEX idx_llm_eval_scores_request_id ON llm_evaluation_scores(request_id);
CREATE INDEX idx_llm_eval_scores_name ON llm_evaluation_scores(score_name);

-- ============================================================================
-- Sessions: Logical grouping of related LLM requests
-- (Metadata only - actual session data aggregated from ClickHouse)
-- ============================================================================

CREATE TABLE llm_sessions_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    -- Session identification (from gen_ai.session.id attribute)
    session_id VARCHAR(255) NOT NULL,
    
    -- Optional user association
    user_id VARCHAR(255),                    -- From gen_ai.user.id or custom property
    
    -- Session-level feedback
    feedback_score INT,                      -- 1-5 rating
    feedback_text TEXT,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE(project_id, session_id)
);

CREATE INDEX idx_llm_sessions_project_id ON llm_sessions_metadata(project_id);
CREATE INDEX idx_llm_sessions_session_id ON llm_sessions_metadata(session_id);
CREATE INDEX idx_llm_sessions_user_id ON llm_sessions_metadata(user_id);

-- ============================================================================
-- Usage Billing Schema
-- Cost forecasting and usage billing for Reiver platform
-- ============================================================================

-- ============================================================================
-- Billing Pricing Configuration
-- Per-organization pricing (admin configurable)
-- ============================================================================

CREATE TABLE billing_pricing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Billing data must be preserved for legal/compliance (7+ years for tax records)
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    
    -- Separate pricing per event type (in USD per million events)
    traces_per_million_usd DECIMAL(10, 4) NOT NULL DEFAULT 1.0000,
    logs_per_million_usd DECIMAL(10, 4) NOT NULL DEFAULT 1.0000,
    metrics_per_million_usd DECIMAL(10, 4) NOT NULL DEFAULT 1.0000,
    
    -- Effective date for this pricing (allows historical pricing)
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Audit
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only one active pricing per organization at a time
-- (the one with the latest effective_from <= now())
CREATE INDEX idx_billing_pricing_org ON billing_pricing(organization_id, effective_from DESC);

-- ============================================================================
-- Billing Budgets
-- Per-organization or per-project budget configuration
-- ============================================================================

CREATE TABLE billing_budgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Billing data must be preserved for legal/compliance
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    
    -- Optional project scope (NULL = org-wide budget)
    project_id UUID REFERENCES projects(id) ON DELETE SET NULL,
    
    -- Budget configuration
    monthly_budget_usd DECIMAL(12, 2) NOT NULL,
    
    -- Alert thresholds (percentage of budget)
    -- Must be between 1 and 100 percent
    alert_threshold_percent INTEGER NOT NULL DEFAULT 80
        CONSTRAINT check_alert_threshold_percent CHECK (alert_threshold_percent >= 1 AND alert_threshold_percent <= 100),
    
    -- Enable/disable budget tracking
    enabled BOOLEAN NOT NULL DEFAULT true,
    
    -- Budget alert tracking columns (to prevent repeated notifications)
    last_threshold_alert_at TIMESTAMPTZ,
    last_exceeded_alert_at TIMESTAMPTZ,
    last_alert_percent INTEGER,
    
    -- Audit
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- One budget per org or per project
    UNIQUE (organization_id, project_id)
);

CREATE INDEX idx_billing_budgets_org ON billing_budgets(organization_id, enabled);
CREATE INDEX idx_billing_budgets_project ON billing_budgets(project_id) WHERE project_id IS NOT NULL;

-- ============================================================================
-- Usage Daily Snapshots
-- Source of truth for billing - rolled up from ClickHouse hourly data
-- ============================================================================

CREATE TABLE usage_daily_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Usage data is source of truth for billing - must be preserved
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    
    -- Date of the snapshot
    date DATE NOT NULL,
    
    -- Event counts by type (for reporting breakdown)
    spans_count BIGINT NOT NULL DEFAULT 0,
    logs_count BIGINT NOT NULL DEFAULT 0,
    metrics_count BIGINT NOT NULL DEFAULT 0,
    
    -- Pre-computed total
    total_events BIGINT NOT NULL DEFAULT 0,
    
    -- Cost estimate based on pricing at snapshot time
    estimated_cost_usd DECIMAL(12, 4) NOT NULL DEFAULT 0.0000,
    
    -- Timestamp when this snapshot was created
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- One snapshot per project per day
    UNIQUE (organization_id, project_id, date)
);

CREATE INDEX idx_usage_daily_org_date ON usage_daily_snapshots(organization_id, date DESC);
CREATE INDEX idx_usage_daily_project_date ON usage_daily_snapshots(project_id, date DESC);

-- ============================================================================
-- Billing Functions and Triggers
-- ============================================================================

-- Function to update updated_at timestamp for billing tables
CREATE OR REPLACE FUNCTION update_billing_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_billing_pricing_updated_at
    BEFORE UPDATE ON billing_pricing
    FOR EACH ROW
    EXECUTE FUNCTION update_billing_updated_at();

CREATE TRIGGER trigger_billing_budgets_updated_at
    BEFORE UPDATE ON billing_budgets
    FOR EACH ROW
    EXECUTE FUNCTION update_billing_updated_at();

-- ============================================================================
-- Payment Methods Schema
-- Support for multiple payment providers (starting with Stripe)
-- ============================================================================

-- Payment provider enum
CREATE TYPE payment_provider AS ENUM (
    'stripe',
    'paypal',       -- Future support
    'wire_transfer' -- Future support for enterprise
);

-- Payment method status
CREATE TYPE payment_method_status AS ENUM (
    'active',      -- Ready to use
    'pending',     -- Awaiting verification
    'expired',     -- Card expired or method invalid
    'failed',      -- Setup failed
    'canceled'     -- User canceled
);

-- ============================================================================
-- Payment Methods Table
-- Stores payment method information for organizations
-- ============================================================================

CREATE TABLE payment_methods (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Which organization owns this payment method
    -- RESTRICT: Payment history must be preserved for legal/compliance
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    
    -- Payment provider details
    provider payment_provider NOT NULL DEFAULT 'stripe',
    status payment_method_status NOT NULL DEFAULT 'pending',
    
    -- Provider-specific identifiers (encrypted sensitive data)
    -- For Stripe: customer_id, payment_method_id, subscription_id
    provider_customer_id TEXT,         -- e.g., cus_xxx for Stripe
    provider_payment_method_id TEXT,   -- e.g., pm_xxx for Stripe
    provider_subscription_id TEXT,     -- e.g., sub_xxx for Stripe subscription
    
    -- Display info (safe to show in UI)
    display_name TEXT,                 -- e.g., "Visa ending in 4242"
    card_brand TEXT,                   -- e.g., "visa", "mastercard", "amex"
    card_last_four TEXT,               -- Last 4 digits of card
    card_exp_month INTEGER,            -- Expiration month
    card_exp_year INTEGER,             -- Expiration year
    
    -- Billing details
    billing_email TEXT,
    billing_name TEXT,
    billing_address_line1 TEXT,
    billing_address_line2 TEXT,
    billing_city TEXT,
    billing_state TEXT,
    billing_postal_code TEXT,
    billing_country TEXT,              -- ISO 2-letter country code
    
    -- Whether this is the default payment method for the organization
    is_default BOOLEAN NOT NULL DEFAULT false,
    
    -- Metadata and audit
    metadata JSONB DEFAULT '{}',       -- Provider-specific metadata
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_payment_methods_org ON payment_methods(organization_id);
CREATE INDEX idx_payment_methods_provider ON payment_methods(provider, status);
CREATE INDEX idx_payment_methods_customer ON payment_methods(provider_customer_id);

-- Unique constraint on provider payment method ID (prevents race conditions)
CREATE UNIQUE INDEX idx_payment_methods_provider_pm_id_unique 
    ON payment_methods(provider_payment_method_id) 
    WHERE provider_payment_method_id IS NOT NULL;

-- Ensure only one default payment method per organization
CREATE UNIQUE INDEX idx_payment_methods_default 
    ON payment_methods(organization_id) 
    WHERE is_default = true;

-- Partial index for active payment methods query pattern
CREATE INDEX idx_payment_methods_active 
    ON payment_methods(organization_id) 
    WHERE status = 'active';

-- ============================================================================
-- Update Organizations Table
-- Add default payment method reference and soft delete support
-- ============================================================================

ALTER TABLE organizations 
    ADD COLUMN default_payment_method_id UUID REFERENCES payment_methods(id) ON DELETE SET NULL,
    ADD COLUMN deleted_at TIMESTAMPTZ,
    ADD COLUMN deletion_reason TEXT,
    ADD COLUMN deleted_by UUID REFERENCES users(id);

CREATE INDEX idx_organizations_payment ON organizations(default_payment_method_id) 
    WHERE default_payment_method_id IS NOT NULL;

-- Index for finding deleted organizations
CREATE INDEX idx_organizations_deleted 
    ON organizations(deleted_at) 
    WHERE deleted_at IS NOT NULL;

-- Index for finding active organizations (most common query)
CREATE INDEX idx_organizations_active 
    ON organizations(id) 
    WHERE deleted_at IS NULL;

-- ============================================================================
-- Prevent Hard Delete Trigger for Organizations
-- Defense-in-depth: even if application code tries to DELETE, prevent it
-- ============================================================================

CREATE OR REPLACE FUNCTION prevent_org_hard_delete()
RETURNS TRIGGER AS $$
BEGIN
    -- Always prevent hard delete - use soft delete instead
    RAISE EXCEPTION 'Hard delete of organizations is not allowed. Use soft delete (UPDATE SET deleted_at = NOW()) instead. Billing data must be preserved for legal/compliance reasons.';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_prevent_org_hard_delete
    BEFORE DELETE ON organizations
    FOR EACH ROW EXECUTE FUNCTION prevent_org_hard_delete();

-- ============================================================================
-- Stripe Customers Table
-- Track Stripe customer IDs per organization for easy lookup
-- ============================================================================

CREATE TABLE stripe_customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Customer records linked to billing data - must be preserved
    organization_id UUID NOT NULL UNIQUE REFERENCES organizations(id) ON DELETE RESTRICT,
    stripe_customer_id TEXT NOT NULL UNIQUE,  -- e.g., cus_xxx
    
    -- Sync status
    email TEXT,
    name TEXT,
    currency TEXT DEFAULT 'usd',
    
    -- Metadata
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stripe_customers_stripe_id ON stripe_customers(stripe_customer_id);

-- ============================================================================
-- Stripe Subscriptions Table
-- Track subscription state separately for billing cycles
-- ============================================================================

CREATE TABLE stripe_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Subscription history must be preserved for billing audit
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    stripe_subscription_id TEXT NOT NULL UNIQUE,  -- e.g., sub_xxx
    -- FK to stripe_customers to ensure data integrity
    stripe_customer_id TEXT NOT NULL REFERENCES stripe_customers(stripe_customer_id) ON DELETE RESTRICT,
    
    -- Subscription status (mirrors Stripe's status)
    status TEXT NOT NULL,  -- 'active', 'past_due', 'canceled', 'incomplete', etc.
    
    -- Billing period
    current_period_start TIMESTAMPTZ,
    current_period_end TIMESTAMPTZ,
    
    -- Pricing details
    price_id TEXT,                    -- Stripe price ID
    quantity INTEGER DEFAULT 1,
    
    -- Cancellation
    cancel_at_period_end BOOLEAN DEFAULT false,
    canceled_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    
    -- Trial
    trial_start TIMESTAMPTZ,
    trial_end TIMESTAMPTZ,
    
    -- Metadata
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stripe_subscriptions_org ON stripe_subscriptions(organization_id);
CREATE INDEX idx_stripe_subscriptions_status ON stripe_subscriptions(status);

-- ============================================================================
-- Stripe Events Table
-- Store webhook events for idempotency and audit trail
-- ============================================================================

CREATE TABLE stripe_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stripe_event_id TEXT NOT NULL UNIQUE,  -- e.g., evt_xxx
    event_type TEXT NOT NULL,              -- e.g., 'invoice.paid', 'customer.subscription.updated'
    
    -- Event data
    data JSONB NOT NULL,
    
    -- Processing status
    processed BOOLEAN NOT NULL DEFAULT false,
    processed_at TIMESTAMPTZ,
    error_message TEXT,
    
    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stripe_events_type ON stripe_events(event_type);
CREATE INDEX idx_stripe_events_processed ON stripe_events(processed) WHERE processed = false;

-- ============================================================================
-- Invoices Table
-- Track billing invoices (can come from Stripe or be generated internally)
-- ============================================================================

CREATE TABLE invoices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- RESTRICT: Invoices must be preserved for legal/tax compliance (7+ years)
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    
    -- Invoice details
    invoice_number TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',  -- 'draft', 'open', 'paid', 'void', 'uncollectible'
    
    -- Provider details (if from Stripe)
    provider payment_provider,
    provider_invoice_id TEXT,              -- e.g., in_xxx for Stripe
    
    -- Amounts (in cents/smallest currency unit)
    subtotal_cents BIGINT NOT NULL DEFAULT 0,
    tax_cents BIGINT NOT NULL DEFAULT 0,
    total_cents BIGINT NOT NULL DEFAULT 0,
    amount_paid_cents BIGINT NOT NULL DEFAULT 0,
    amount_due_cents BIGINT NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'usd',
    
    -- Billing period
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    
    -- Due date and payment
    due_date TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    
    -- PDF URL (from Stripe or generated)
    invoice_pdf_url TEXT,
    hosted_invoice_url TEXT,
    
    -- Line items stored as JSONB for flexibility
    line_items JSONB DEFAULT '[]',
    
    -- Metadata
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_invoices_org ON invoices(organization_id);
CREATE INDEX idx_invoices_status ON invoices(status);
CREATE UNIQUE INDEX idx_invoices_number ON invoices(organization_id, invoice_number);

-- Unique partial index on provider_invoice_id for Stripe invoice deduplication
CREATE UNIQUE INDEX idx_invoices_provider_unique 
    ON invoices(provider_invoice_id) 
    WHERE provider_invoice_id IS NOT NULL;

-- ============================================================================
-- Orphaned Invoices Table
-- Stores invoices from Stripe webhooks that couldn't be matched to a customer
-- ============================================================================

CREATE TABLE orphaned_invoices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Stripe identifiers (unique to prevent duplicates from webhook retries)
    stripe_invoice_id TEXT NOT NULL UNIQUE,
    stripe_customer_id TEXT NOT NULL,
    
    -- Invoice details from webhook
    invoice_number TEXT,
    total_cents BIGINT NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'usd',
    status TEXT NOT NULL,
    
    -- Period information
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    
    -- URLs
    invoice_pdf_url TEXT,
    hosted_invoice_url TEXT,
    
    -- Full webhook payload for investigation
    webhook_payload JSONB NOT NULL,
    
    -- Processing status
    resolved BOOLEAN NOT NULL DEFAULT false,
    resolved_at TIMESTAMPTZ,
    resolved_by UUID REFERENCES users(id),
    resolution_notes TEXT,
    
    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orphaned_invoices_unresolved 
    ON orphaned_invoices(created_at) 
    WHERE resolved = false;

CREATE INDEX idx_orphaned_invoices_customer 
    ON orphaned_invoices(stripe_customer_id);

CREATE INDEX idx_orphaned_invoices_resolved 
    ON orphaned_invoices(resolved) 
    WHERE resolved = false;

CREATE INDEX idx_orphaned_invoices_resolved_at 
    ON orphaned_invoices(resolved_at) 
    WHERE resolved = true;

CREATE INDEX idx_orphaned_invoices_stale_unresolved 
    ON orphaned_invoices(created_at) 
    WHERE resolved = false;

-- ============================================================================
-- Orphaned Subscriptions Table
-- Stores subscription events from Stripe webhooks that couldn't be matched to a customer
-- ============================================================================

CREATE TABLE orphaned_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Stripe identifiers (unique to prevent duplicates from webhook retries)
    stripe_subscription_id TEXT NOT NULL UNIQUE,
    stripe_customer_id TEXT NOT NULL,
    
    -- Subscription details from webhook
    status TEXT NOT NULL,
    current_period_start TIMESTAMPTZ,
    current_period_end TIMESTAMPTZ,
    cancel_at_period_end BOOLEAN DEFAULT false,
    
    -- Price information
    price_id TEXT,
    
    -- Full webhook payload for investigation
    webhook_payload JSONB NOT NULL,
    
    -- The event that triggered this orphaned record
    stripe_event_id TEXT,
    
    -- Processing status
    resolved BOOLEAN NOT NULL DEFAULT false,
    resolved_at TIMESTAMPTZ,
    resolved_by UUID REFERENCES users(id),
    resolution_notes TEXT,
    
    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orphaned_subscriptions_unresolved 
    ON orphaned_subscriptions(created_at) 
    WHERE resolved = false;

CREATE INDEX idx_orphaned_subscriptions_customer 
    ON orphaned_subscriptions(stripe_customer_id);

CREATE INDEX idx_orphaned_subscriptions_resolved 
    ON orphaned_subscriptions(resolved) 
    WHERE resolved = false;

CREATE INDEX idx_orphaned_subscriptions_resolved_at 
    ON orphaned_subscriptions(resolved_at) 
    WHERE resolved = true;

CREATE INDEX idx_orphaned_subscriptions_stale_unresolved 
    ON orphaned_subscriptions(created_at) 
    WHERE resolved = false;

-- ============================================================================
-- Payment Triggers
-- ============================================================================

-- Update updated_at timestamp for payment tables
CREATE OR REPLACE FUNCTION update_payment_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_payment_methods_updated_at
    BEFORE UPDATE ON payment_methods
    FOR EACH ROW
    EXECUTE FUNCTION update_payment_updated_at();

CREATE TRIGGER trigger_stripe_customers_updated_at
    BEFORE UPDATE ON stripe_customers
    FOR EACH ROW
    EXECUTE FUNCTION update_payment_updated_at();

CREATE TRIGGER trigger_stripe_subscriptions_updated_at
    BEFORE UPDATE ON stripe_subscriptions
    FOR EACH ROW
    EXECUTE FUNCTION update_payment_updated_at();

CREATE TRIGGER trigger_invoices_updated_at
    BEFORE UPDATE ON invoices
    FOR EACH ROW
    EXECUTE FUNCTION update_payment_updated_at();


-- ============================================================================
-- 002_watch_initial_schema.sql
-- ============================================================================

-- Watch (APM) PostgreSQL Schema
-- Extracted from the consolidated Reiver schema
-- Tables for error tracing, cloud integrations, and database monitoring

-- ============================================================================
-- Error-to-Trace Correlation
-- ============================================================================

-- Error-to-trace correlation
CREATE TABLE error_traces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    error_id VARCHAR(255) NOT NULL,
    trace_id VARCHAR(255) NOT NULL,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    span_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, error_id, trace_id)
);

CREATE INDEX idx_error_traces_error_id ON error_traces(error_id);
CREATE INDEX idx_error_traces_trace_id ON error_traces(trace_id);
CREATE INDEX idx_error_traces_project_id ON error_traces(project_id);
CREATE INDEX idx_error_traces_lookup ON error_traces(project_id, error_id, trace_id);

-- ============================================================================
-- Cloud Integration Configs
-- ============================================================================

-- AWS Integration Configs
CREATE TABLE aws_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    integration_type VARCHAR(50) NOT NULL,
    
    region VARCHAR(50) NOT NULL DEFAULT 'us-east-1',
    access_key_id TEXT,
    secret_access_key_encrypted TEXT,
    session_token TEXT,
    role_arn TEXT,
    external_id TEXT,
    
    config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 300,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_aws_integration_configs_project ON aws_integration_configs(project_id, enabled);
CREATE INDEX idx_aws_integration_configs_type ON aws_integration_configs(integration_type, enabled);

-- OCI Integration Configs
CREATE TABLE oci_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    integration_type VARCHAR(50) NOT NULL,
    
    tenancy_ocid TEXT NOT NULL,
    user_ocid TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    private_key TEXT NOT NULL,
    region VARCHAR(50) NOT NULL DEFAULT 'us-ashburn-1',
    passphrase TEXT,
    
    config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 300,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_oci_integration_configs_project ON oci_integration_configs(project_id, enabled);
CREATE INDEX idx_oci_integration_configs_type ON oci_integration_configs(integration_type, enabled);

-- Azure Integration Configs
CREATE TABLE azure_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    integration_type VARCHAR(50) NOT NULL,
    
    subscription_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255),
    client_id VARCHAR(255),
    client_secret TEXT, -- In production, should be encrypted
    
    config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 300,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_azure_integration_configs_project ON azure_integration_configs(project_id, enabled);
CREATE INDEX idx_azure_integration_configs_type ON azure_integration_configs(integration_type, enabled);

-- GCP Integration Configs
CREATE TABLE gcp_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    integration_type VARCHAR(50) NOT NULL,
    
    gcp_project_id VARCHAR(255) NOT NULL,
    service_account_email VARCHAR(255),
    private_key TEXT, -- In production, should be encrypted
    service_account_json TEXT, -- In production, should be encrypted
    
    config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 300,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_gcp_integration_configs_project ON gcp_integration_configs(project_id, enabled);
CREATE INDEX idx_gcp_integration_configs_type ON gcp_integration_configs(integration_type, enabled);

-- Snowflake Integration Configs
CREATE TABLE snowflake_integration_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    integration_type VARCHAR(50) NOT NULL,
    
    account VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password TEXT NOT NULL, -- In production, should be encrypted
    warehouse VARCHAR(255),
    database VARCHAR(255),
    schema VARCHAR(255),
    role VARCHAR(255),
    
    config_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 300,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_snowflake_integration_configs_project ON snowflake_integration_configs(project_id, enabled);
CREATE INDEX idx_snowflake_integration_configs_type ON snowflake_integration_configs(integration_type, enabled);

-- ============================================================================
-- Database Monitoring
-- ============================================================================

-- Database Monitoring Configs
CREATE TABLE database_monitoring_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    database_type VARCHAR(50) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password_encrypted TEXT,
    
    enabled BOOLEAN NOT NULL DEFAULT true,
    collection_interval_seconds INTEGER NOT NULL DEFAULT 60,
    slow_query_threshold_ms DOUBLE PRECISION DEFAULT 1000.0,
    
    pg_stat_statements_enabled BOOLEAN DEFAULT true,
    pg_stat_statements_limit INTEGER DEFAULT 10000,
    
    performance_schema_enabled BOOLEAN DEFAULT true,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, name)
);

CREATE INDEX idx_db_monitoring_configs_project ON database_monitoring_configs(project_id, enabled);

-- Database Query Metrics
CREATE TABLE database_query_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    database_host VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    database_type VARCHAR(50) NOT NULL,
    
    query_fingerprint VARCHAR(500) NOT NULL,
    query_template TEXT NOT NULL,
    
    calls BIGINT NOT NULL,
    total_time_ms DOUBLE PRECISION NOT NULL,
    mean_time_ms DOUBLE PRECISION NOT NULL,
    min_time_ms DOUBLE PRECISION NOT NULL,
    max_time_ms DOUBLE PRECISION NOT NULL,
    stddev_time_ms DOUBLE PRECISION,
    rows_affected BIGINT,
    rows_returned BIGINT,
    
    shared_blks_hit BIGINT,
    shared_blks_read BIGINT,
    temp_blks_read BIGINT,
    temp_blks_written BIGINT,
    blk_read_time_ms DOUBLE PRECISION,
    blk_write_time_ms DOUBLE PRECISION,
    
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    UNIQUE (project_id, database_host, database_name, query_fingerprint, collected_at)
);

CREATE INDEX idx_query_metrics_project ON database_query_metrics(project_id, collected_at DESC);
CREATE INDEX idx_query_metrics_slow ON database_query_metrics(project_id, mean_time_ms DESC);
CREATE INDEX idx_query_metrics_fingerprint ON database_query_metrics(project_id, query_fingerprint, collected_at DESC);

-- Database Explain Plans
CREATE TABLE database_explain_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    query_metric_id UUID REFERENCES database_query_metrics(id) ON DELETE CASCADE,
    database_host VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    query_template TEXT NOT NULL,
    query_parameters JSONB,
    
    explain_plan JSONB NOT NULL,
    
    execution_time_ms DOUBLE PRECISION,
    planning_time_ms DOUBLE PRECISION,
    total_cost DOUBLE PRECISION,
    rows_estimated BIGINT,
    rows_actual BIGINT,
    
    has_full_table_scan BOOLEAN DEFAULT false,
    has_missing_index BOOLEAN DEFAULT false,
    has_sequential_scan BOOLEAN DEFAULT false,
    
    trace_id VARCHAR(255),
    
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_explain_plans_project ON database_explain_plans(project_id, collected_at DESC);
CREATE INDEX idx_explain_plans_trace ON database_explain_plans(trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX idx_explain_plans_issues ON database_explain_plans(project_id, has_full_table_scan, has_missing_index);

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE error_traces IS 'Junction table linking errors to traces for correlation';
COMMENT ON TABLE database_query_metrics IS 'Query performance metrics collected from database performance tables';
COMMENT ON TABLE database_explain_plans IS 'Explain plans for slow queries to diagnose performance issues';
COMMENT ON TABLE database_monitoring_configs IS 'Configuration for which databases to monitor and how';

-- ============================================================================
-- 003_watch_game_observability.sql
-- ============================================================================

-- ============================================================================
-- Game Development Observability Features
-- Dashboard templates, alert rule templates, and game-specific extensions
-- ============================================================================

-- ============================================================================
-- Alert Rule Templates (pre-built alert configurations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS alert_rule_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    category VARCHAR(100) NOT NULL DEFAULT 'general',
    
    -- Template configuration (same structure as alert_rules.query_config)
    query_config JSONB NOT NULL,
    
    -- Default threshold
    default_threshold FLOAT8 NOT NULL DEFAULT 0,
    default_threshold_type VARCHAR(10) NOT NULL DEFAULT 'above',
    
    -- Default evaluation settings
    default_eval_window_seconds INTEGER NOT NULL DEFAULT 300,
    default_eval_interval_seconds INTEGER NOT NULL DEFAULT 60,
    
    -- Default alert on absent
    default_alert_on_absent BOOLEAN DEFAULT false,
    default_absent_for_seconds INTEGER DEFAULT 300,
    
    -- Metadata
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    is_featured BOOLEAN DEFAULT false,
    display_order INTEGER DEFAULT 0,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_rule_templates_category ON alert_rule_templates(category);
CREATE INDEX IF NOT EXISTS idx_alert_rule_templates_tags ON alert_rule_templates USING GIN(tags);

-- ============================================================================
-- Game Dashboard Templates
-- ============================================================================

-- Multiplayer Game Server Dashboard
INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Game Server Dashboard',
    'Real-time monitoring for multiplayer game servers: tick rate, player count, match metrics, and network quality',
    'gaming',
    true,
    10,
    ARRAY['gaming', 'multiplayer', 'server', 'real-time'],
    '{
        "variables": [
            {"name": "server_region", "label": "Server Region", "type": "select", "default": "", "options": ["us-west-2", "us-east-1", "eu-central-1", "ap-northeast-1"]},
            {"name": "match_mode", "label": "Match Mode", "type": "select", "default": "", "options": ["ranked", "casual", "tutorial"]}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "gamepad",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Active Matches",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "last", "field": "value", "alias": "matches"}],
                                "where": "metric_name = ''game.server.match.count''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Active Players",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "last", "field": "value", "alias": "players"}],
                                "where": "metric_name = ''game.server.player.count''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Tick Rate",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "Hz",
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "avg", "field": "value", "alias": "tick_rate"}],
                                "where": "metric_name = ''game.server.tick.rate''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Player RTT",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "ms",
                            "transform": "multiply_1000",
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "avg", "field": "value", "alias": "rtt"}],
                                "where": "metric_name = ''game.network.rtt''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Server Tick Rate Over Time",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "unit": "Hz",
                            "thresholds": [{"value": 20, "color": "red", "label": "Min acceptable"}],
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "avg", "field": "value", "alias": "avg_tick_rate"},
                                    {"fn": "min", "field": "value", "alias": "min_tick_rate"},
                                    {"fn": "max", "field": "value", "alias": "max_tick_rate"}
                                ],
                                "where": "metric_name = ''game.server.tick.rate''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Player Count Over Time",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "displayMode": "area",
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "max", "field": "value", "alias": "players"}],
                                "where": "metric_name = ''game.server.player.count''",
                                "groupBy": ["metric_attributes[''game.server.region'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Match Starts / Ends",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["span_name = ''game.match.start''"], "alias": "starts"},
                                    {"fn": "countIf", "args": ["span_name = ''game.match.end''"], "alias": "ends"}
                                ],
                                "where": "span_attributes[''game.match.id''] != ''''",
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Network Quality (RTT & Packet Loss)",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "quantile", "args": [0.50], "field": "value", "alias": "p50_rtt"},
                                    {"fn": "quantile", "args": [0.95], "field": "value", "alias": "p95_rtt"}
                                ],
                                "where": "metric_name = ''game.network.rtt''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Active Matches by Region",
                        "x": 0, "y": 10, "w": 12, "h": 4,
                        "config": {
                            "sortable": true,
                            "query": {
                                "table": "game_matches",
                                "select": [
                                    {"field": "server_region", "alias": "region"},
                                    {"field": "match_mode", "alias": "mode"},
                                    {"fn": "count", "alias": "active_matches"},
                                    {"fn": "sum", "field": "player_count", "alias": "total_players"},
                                    {"fn": "avg", "field": "avg_server_tick_rate", "alias": "avg_tick_rate"}
                                ],
                                "where": "outcome = ''ongoing''",
                                "groupBy": ["server_region", "match_mode"],
                                "orderBy": "active_matches DESC"
                            }
                        }
                    }
                ]
            },
            {
                "name": "Performance",
                "icon": "chart",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Tick Duration Distribution",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "unit": "ms",
                            "transform": "multiply_1000",
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "quantile", "args": [0.50], "field": "value", "alias": "p50"},
                                    {"fn": "quantile", "args": [0.95], "field": "value", "alias": "p95"},
                                    {"fn": "quantile", "args": [0.99], "field": "value", "alias": "p99"}
                                ],
                                "where": "metric_name = ''game.server.tick.duration''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "histogram",
                        "title": "Match Duration Distribution",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "unit": "min",
                            "query": {
                                "table": "game_matches",
                                "select": [{"fn": "histogram", "field": "duration_seconds / 60", "buckets": 20}],
                                "where": "outcome = ''completed''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Server Errors Over Time",
                        "x": 0, "y": 4, "w": 12, "h": 4,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "errors"}],
                                "where": "status_code = ''STATUS_CODE_ERROR'' AND span_attributes[''game.match.id''] != ''''",
                                "groupBy": ["span_name"],
                                "interval": "5m"
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
),
-- Mobile Game Client Dashboard
(
    'Mobile Game Client Dashboard',
    'Client-side performance monitoring for mobile games: FPS, frame times, crashes, memory, and session quality',
    'gaming',
    true,
    11,
    ARRAY['gaming', 'mobile', 'client', 'performance'],
    '{
        "variables": [
            {"name": "platform", "label": "Platform", "type": "select", "default": "", "options": ["android", "ios"]},
            {"name": "game_version", "label": "Game Version", "type": "text", "default": ""}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "smartphone",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "Avg FPS",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "fps",
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "avg", "field": "value", "alias": "fps"}],
                                "where": "metric_name = ''game.client.frame.rate''"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Crash-Free Sessions",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "%",
                            "query": {
                                "table": "game_player_sessions",
                                "select": [
                                    {"expr": "countIf(end_reason != ''crash'') * 100.0 / count()", "alias": "crash_free_rate"}
                                ]
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Active Sessions",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "game_player_sessions",
                                "select": [{"fn": "count", "alias": "sessions"}],
                                "where": "end_time = toDateTime64(0, 9)"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Session Quality",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "unit": "/100",
                            "query": {
                                "table": "game_player_sessions",
                                "select": [{"fn": "avg", "field": "quality_score", "alias": "quality"}]
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "FPS Distribution Over Time",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "unit": "fps",
                            "thresholds": [
                                {"value": 60, "color": "green", "label": "Target"},
                                {"value": 30, "color": "yellow", "label": "Acceptable"},
                                {"value": 15, "color": "red", "label": "Poor"}
                            ],
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "quantile", "args": [0.50], "field": "value", "alias": "p50_fps"},
                                    {"fn": "quantile", "args": [0.05], "field": "value", "alias": "p5_fps"},
                                    {"fn": "quantile", "args": [0.95], "field": "value", "alias": "p95_fps"}
                                ],
                                "where": "metric_name = ''game.client.frame.rate''",
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Frame Time (P95)",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "unit": "ms",
                            "transform": "multiply_1000",
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "quantile", "args": [0.95], "field": "value", "alias": "p95_frame_time"}
                                ],
                                "where": "metric_name = ''game.client.frame.duration''",
                                "groupBy": ["resource_attributes[''game.platform'']"],
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Crashes Over Time",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "game_player_sessions",
                                "select": [{"fn": "countIf", "args": ["end_reason = ''crash''"], "alias": "crashes"}],
                                "groupBy": ["platform"],
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "unit": "MB",
                            "transform": "divide_1048576",
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "avg", "field": "value", "alias": "avg_memory"},
                                    {"fn": "max", "field": "value", "alias": "peak_memory"}
                                ],
                                "where": "metric_name = ''game.client.memory.usage''",
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "FPS by Device Tier",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "avg", "field": "value", "alias": "avg_fps"}
                                ],
                                "where": "metric_name = ''game.client.frame.rate''",
                                "groupBy": ["resource_attributes[''device.model.name'']"],
                                "orderBy": "avg_fps DESC",
                                "limit": 15
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Session End Reasons",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "sortable": true,
                            "query": {
                                "table": "game_player_sessions",
                                "select": [
                                    {"field": "end_reason", "alias": "reason"},
                                    {"fn": "count", "alias": "count"},
                                    {"expr": "count() * 100.0 / sum(count()) OVER ()", "alias": "percentage"}
                                ],
                                "where": "end_reason != ''''",
                                "groupBy": ["end_reason"],
                                "orderBy": "count DESC"
                            }
                        }
                    }
                ]
            },
            {
                "name": "Network",
                "icon": "wifi",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Client RTT Over Time",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "unit": "ms",
                            "transform": "multiply_1000",
                            "query": {
                                "table": "samples_v1",
                                "select": [
                                    {"fn": "quantile", "args": [0.50], "field": "value", "alias": "p50"},
                                    {"fn": "quantile", "args": [0.95], "field": "value", "alias": "p95"}
                                ],
                                "where": "metric_name = ''game.network.rtt''",
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Packet Loss Rate",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "unit": "%",
                            "transform": "multiply_100",
                            "thresholds": [{"value": 2, "color": "red", "label": "Critical"}],
                            "query": {
                                "table": "samples_v1",
                                "select": [{"fn": "avg", "field": "value", "alias": "packet_loss"}],
                                "where": "metric_name = ''game.network.packet_loss''",
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "geomap",
                        "title": "Player Latency by Region",
                        "x": 0, "y": 4, "w": 12, "h": 6,
                        "config": {
                            "metricField": "avg_rtt",
                            "query": {
                                "table": "game_player_sessions",
                                "select": [
                                    {"field": "country_code", "alias": "country"},
                                    {"fn": "avg", "field": "avg_rtt_seconds * 1000", "alias": "avg_rtt"},
                                    {"fn": "count", "alias": "players"}
                                ],
                                "groupBy": ["country_code"],
                                "orderBy": "players DESC"
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
),
-- Live Ops Dashboard
(
    'Game Live Ops Dashboard',
    'Real-time operational metrics for game live services: CCU, match activity, revenue events, and feature flag impact',
    'gaming',
    true,
    12,
    ARRAY['gaming', 'live-ops', 'operations', 'real-time'],
    '{
        "variables": [
            {"name": "match_mode", "label": "Match Mode", "type": "select", "default": "", "options": ["ranked", "casual", "tutorial", "event"]}
        ],
        "tabs": [
            {
                "name": "Live Metrics",
                "icon": "activity",
                "widgets": [
                    {
                        "type": "metric",
                        "title": "CCU (Concurrent Users)",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "query": {
                                "table": "game_player_sessions",
                                "select": [{"fn": "count", "alias": "ccu"}],
                                "where": "end_time = toDateTime64(0, 9)"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Matches/Min",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "query": {
                                "table": "game_matches",
                                "select": [{"fn": "count", "alias": "matches"}],
                                "where": "start_time > now() - INTERVAL 1 MINUTE"
                            }
                        }
                    },
                    {
                        "type": "metric",
                        "title": "Avg Queue Wait",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "unit": "s",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "avg", "field": "duration / 1000000000", "alias": "wait_time"}],
                                "where": "span_name = ''matchmaking.queue''"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CCU Over Time",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "displayMode": "area",
                            "query": {
                                "table": "game_metrics_hourly",
                                "select": [{"fn": "sum", "field": "player_sessions", "alias": "ccu"}],
                                "groupBy": ["match_mode"],
                                "interval": "1h"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Match Starts by Mode",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "game_matches",
                                "select": [{"fn": "count", "alias": "matches"}],
                                "groupBy": ["match_mode"],
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Match Abandonment Rate",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "unit": "%",
                            "query": {
                                "table": "game_matches",
                                "select": [
                                    {"expr": "countIf(outcome = ''abandoned'') * 100.0 / count()", "alias": "abandonment_rate"}
                                ],
                                "interval": "15m"
                            }
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Players by Region",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "game_player_sessions",
                                "select": [{"fn": "count", "alias": "players"}],
                                "where": "end_time = toDateTime64(0, 9)",
                                "groupBy": ["country_code"],
                                "orderBy": "players DESC",
                                "limit": 10
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Top Maps by Player Count",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "sortable": true,
                            "query": {
                                "table": "game_matches",
                                "select": [
                                    {"field": "match_map", "alias": "map"},
                                    {"fn": "sum", "field": "player_count", "alias": "total_players"},
                                    {"fn": "count", "alias": "match_count"},
                                    {"fn": "avg", "field": "duration_seconds / 60", "alias": "avg_duration_min"}
                                ],
                                "where": "start_time > now() - INTERVAL 1 HOUR",
                                "groupBy": ["match_map"],
                                "orderBy": "total_players DESC",
                                "limit": 10
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    is_featured = EXCLUDED.is_featured,
    display_order = EXCLUDED.display_order,
    tags = EXCLUDED.tags,
    template_config = EXCLUDED.template_config,
    updated_at = NOW();

-- ============================================================================
-- Game Alert Rule Templates
-- ============================================================================
INSERT INTO alert_rule_templates (name, description, category, query_config, default_threshold, default_threshold_type, default_eval_window_seconds, default_eval_interval_seconds, tags, is_featured, display_order) VALUES
(
    'Server Tick Rate Low',
    'Alert when game server tick rate drops below threshold, indicating server performance issues',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.server.tick.rate",
        "time_aggregation": "avg",
        "space_aggregation": "min"
    }'::jsonb,
    20,
    'below',
    60,
    30,
    ARRAY['gaming', 'server', 'performance'],
    true,
    1
),
(
    'Client FPS Low (P95)',
    'Alert when 95th percentile client FPS drops, indicating widespread client performance issues',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.client.frame.rate",
        "time_aggregation": "p95",
        "space_aggregation": "avg"
    }'::jsonb,
    30,
    'below',
    300,
    60,
    ARRAY['gaming', 'client', 'performance'],
    true,
    2
),
(
    'Network Packet Loss High',
    'Alert when packet loss ratio exceeds threshold, indicating network quality issues',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.network.packet_loss",
        "time_aggregation": "avg",
        "space_aggregation": "max"
    }'::jsonb,
    0.02,
    'above',
    120,
    30,
    ARRAY['gaming', 'network', 'quality'],
    true,
    3
),
(
    'Match Abandonment Rate High',
    'Alert when match abandonment rate exceeds threshold, indicating player experience issues',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.match.abandonment_rate",
        "time_aggregation": "avg",
        "space_aggregation": "sum",
        "filters": {"outcome": "abandoned"}
    }'::jsonb,
    0.15,
    'above',
    900,
    300,
    ARRAY['gaming', 'live-ops', 'player-experience'],
    true,
    4
),
(
    'Client Crash Rate High',
    'Alert when client crash rate exceeds threshold by game version',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.client.crash_rate",
        "time_aggregation": "sum",
        "space_aggregation": "sum",
        "group_by": ["game.version"]
    }'::jsonb,
    0.01,
    'above',
    3600,
    600,
    ARRAY['gaming', 'client', 'stability'],
    true,
    5
),
(
    'Matchmaking Queue Time High',
    'Alert when average matchmaking queue time exceeds threshold',
    'gaming',
    '{
        "query_type": "spans",
        "span_name": "matchmaking.queue",
        "time_aggregation": "p95",
        "space_aggregation": "avg"
    }'::jsonb,
    60,
    'above',
    300,
    60,
    ARRAY['gaming', 'matchmaking', 'player-experience'],
    false,
    6
),
(
    'Server Tick Duration High (P99)',
    'Alert when 99th percentile tick duration exceeds frame budget',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.server.tick.duration",
        "time_aggregation": "p99",
        "space_aggregation": "max"
    }'::jsonb,
    0.033,
    'above',
    60,
    30,
    ARRAY['gaming', 'server', 'performance'],
    false,
    7
),
(
    'Player RTT High (Regional)',
    'Alert when average player RTT exceeds threshold by region',
    'gaming',
    '{
        "query_type": "metrics",
        "metric_name": "game.network.rtt",
        "time_aggregation": "p95",
        "space_aggregation": "avg",
        "group_by": ["game.server.region"]
    }'::jsonb,
    0.1,
    'above',
    300,
    60,
    ARRAY['gaming', 'network', 'regional'],
    false,
    8
)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    query_config = EXCLUDED.query_config,
    default_threshold = EXCLUDED.default_threshold,
    default_threshold_type = EXCLUDED.default_threshold_type,
    default_eval_window_seconds = EXCLUDED.default_eval_window_seconds,
    default_eval_interval_seconds = EXCLUDED.default_eval_interval_seconds,
    tags = EXCLUDED.tags,
    is_featured = EXCLUDED.is_featured,
    display_order = EXCLUDED.display_order,
    updated_at = NOW();

-- ============================================================================
-- 004_watch_github_integration.sql
-- ============================================================================

-- GitHub Integration Schema
-- Adds support for linking projects to GitHub repositories and storing installation credentials

-- GitHub App installations (per organization)
-- Note: Access tokens are not stored here; octocrab handles JWT-based token
-- management internally, generating short-lived installation tokens on demand.
CREATE TABLE github_installations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    installation_id BIGINT NOT NULL UNIQUE,  -- GitHub App installation ID
    account_login VARCHAR(255) NOT NULL,      -- GitHub org/user login (e.g., 'acme-corp')
    account_type VARCHAR(50) NOT NULL,        -- 'Organization' or 'User'
    repositories JSONB DEFAULT '[]',          -- List of accessible repos [{name, full_name, private}]
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_github_installations_org ON github_installations(organization_id);
CREATE INDEX idx_github_installations_account ON github_installations(account_login);

-- GIN index for efficient JSONB containment queries on repositories
-- Used for checking if a repo is accessible: WHERE repositories @> '[{"full_name": "owner/repo"}]'
CREATE INDEX idx_github_installations_repos ON github_installations 
    USING GIN (repositories jsonb_path_ops);

-- (github_repo_url column is defined inline on the projects table)

-- ============================================================================
-- 005_flow_llm_rollouts.sql
-- ============================================================================

-- ============================================================================
-- LLM Prompt Rollouts Schema
-- Progressive deployment with auto-promote/rollback for LLM prompts
-- ============================================================================

-- ============================================================================
-- Prompt Configurations (what can be rolled out)
-- ============================================================================

CREATE TABLE llm_prompt_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    -- Current active version (what 'baseline' traffic uses)
    active_version_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

CREATE INDEX idx_llm_prompt_configs_project_id ON llm_prompt_configs(project_id);
CREATE INDEX idx_llm_prompt_configs_name ON llm_prompt_configs(project_id, name);

-- ============================================================================
-- Prompt Versions (immutable snapshots)
-- ============================================================================

CREATE TABLE llm_prompt_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID NOT NULL REFERENCES llm_prompt_configs(id) ON DELETE CASCADE,
    version INT NOT NULL,
    -- Configuration that affects LLM behavior
    system_prompt TEXT,
    model VARCHAR(100),
    temperature DECIMAL(3,2),
    max_tokens INT,
    parameters JSONB DEFAULT '{}',
    -- Template variable definitions: [{name, description?, var_type, required, default?}]
    variables JSONB DEFAULT '[]',
    -- OpenAI-compatible tool/function definitions for function-calling
    tools JSONB,
    -- JSON schema for structured output (response_format parameter)
    response_format JSONB,
    -- Metadata
    commit_message TEXT,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(config_id, version)
);

CREATE INDEX idx_llm_prompt_versions_config_id ON llm_prompt_versions(config_id);
CREATE INDEX idx_llm_prompt_versions_config_version ON llm_prompt_versions(config_id, version);

-- Add foreign key for active_version_id after llm_prompt_versions exists
ALTER TABLE llm_prompt_configs 
ADD CONSTRAINT fk_llm_prompt_configs_active_version 
FOREIGN KEY (active_version_id) REFERENCES llm_prompt_versions(id);

-- ============================================================================
-- Rollouts (progressive deployment of a version)
-- ============================================================================

CREATE TABLE llm_rollouts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    config_id UUID NOT NULL REFERENCES llm_prompt_configs(id) ON DELETE CASCADE,
    -- What we're rolling out
    target_version_id UUID NOT NULL REFERENCES llm_prompt_versions(id),
    baseline_version_id UUID REFERENCES llm_prompt_versions(id),
    -- Rollout settings
    name VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    mode VARCHAR(20) NOT NULL DEFAULT 'auto',
    allocation_type VARCHAR(20) NOT NULL DEFAULT 'random',
    -- Current state
    current_stage INT DEFAULT 0,
    current_weight INT DEFAULT 0,
    -- Timing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_stage_change_at TIMESTAMPTZ,
    -- Constraints
    CONSTRAINT chk_llm_rollouts_status CHECK (status IN ('pending', 'running', 'paused', 'completed', 'rolled_back')),
    CONSTRAINT chk_llm_rollouts_mode CHECK (mode IN ('auto', 'manual')),
    CONSTRAINT chk_llm_rollouts_allocation CHECK (allocation_type IN ('random', 'user_sticky', 'session_sticky')),
    CONSTRAINT chk_llm_rollouts_weight CHECK (current_weight >= 0 AND current_weight <= 100)
);

CREATE INDEX idx_llm_rollouts_project_id ON llm_rollouts(project_id);
CREATE INDEX idx_llm_rollouts_config_id ON llm_rollouts(config_id);
CREATE INDEX idx_llm_rollouts_status ON llm_rollouts(status);
CREATE INDEX idx_llm_rollouts_project_status ON llm_rollouts(project_id, status);

-- ============================================================================
-- Rollout Stages (progression steps)
-- ============================================================================

CREATE TABLE llm_rollout_stages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rollout_id UUID NOT NULL REFERENCES llm_rollouts(id) ON DELETE CASCADE,
    stage_order INT NOT NULL,
    weight INT NOT NULL,
    min_duration_minutes INT DEFAULT 10,
    min_requests INT DEFAULT 100,
    -- Thresholds for auto-promote (NULL = use defaults)
    max_error_rate_increase DECIMAL(5,2),
    max_latency_increase_pct DECIMAL(5,2),
    min_quality_score DECIMAL(5,2),
    -- State
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    -- Constraints
    UNIQUE(rollout_id, stage_order),
    CONSTRAINT chk_llm_rollout_stages_weight CHECK (weight >= 0 AND weight <= 100),
    CONSTRAINT chk_llm_rollout_stages_status CHECK (status IN ('pending', 'active', 'passed', 'failed'))
);

CREATE INDEX idx_llm_rollout_stages_rollout_id ON llm_rollout_stages(rollout_id);
CREATE INDEX idx_llm_rollout_stages_status ON llm_rollout_stages(rollout_id, status);

-- ============================================================================
-- Rollout Metrics (snapshots for comparison)
-- ============================================================================

CREATE TABLE llm_rollout_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rollout_id UUID NOT NULL REFERENCES llm_rollouts(id) ON DELETE CASCADE,
    stage_order INT NOT NULL,
    variant VARCHAR(20) NOT NULL,
    -- Metrics
    request_count BIGINT DEFAULT 0,
    error_count BIGINT DEFAULT 0,
    error_rate DECIMAL(5,4),
    avg_latency_ms DECIMAL(10,2),
    p95_latency_ms DECIMAL(10,2),
    avg_cost_usd DECIMAL(12,8),
    avg_quality_score DECIMAL(5,4),
    -- Timing
    measured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Constraints
    CONSTRAINT chk_llm_rollout_metrics_variant CHECK (variant IN ('target', 'baseline'))
);

CREATE INDEX idx_llm_rollout_metrics_rollout_id ON llm_rollout_metrics(rollout_id);
CREATE INDEX idx_llm_rollout_metrics_rollout_stage ON llm_rollout_metrics(rollout_id, stage_order);

-- ============================================================================
-- Helper function for updated_at timestamp
-- ============================================================================

CREATE OR REPLACE FUNCTION update_llm_prompt_configs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_llm_prompt_configs_updated_at
    BEFORE UPDATE ON llm_prompt_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_llm_prompt_configs_updated_at();

-- ============================================================================
-- 006_flow_llm_prompt_assets.sql
-- ============================================================================

-- ============================================================================
-- LLM Prompt Assets & Extended Features Schema
-- Multimodal prompt support, template variables, tools, and output schemas
-- ============================================================================

-- ============================================================================
-- Prompt Assets (metadata only - binary data in object storage)
-- ============================================================================

CREATE TABLE llm_prompt_assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    version_id UUID NOT NULL REFERENCES llm_prompt_versions(id) ON DELETE CASCADE,
    -- Asset identification
    filename VARCHAR(255) NOT NULL,
    -- MIME type: image/png, image/jpeg, audio/mp3, application/pdf, etc.
    content_type VARCHAR(100) NOT NULL,
    size_bytes BIGINT NOT NULL,
    -- Storage backend reference
    storage_key VARCHAR(500) NOT NULL,
    storage_backend VARCHAR(50) NOT NULL DEFAULT 'local',
    -- Timing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Constraints
    CONSTRAINT chk_llm_prompt_assets_size CHECK (size_bytes > 0 AND size_bytes <= 20971520), -- 20MB max
    CONSTRAINT chk_llm_prompt_assets_backend CHECK (storage_backend IN ('local', 's3', 'memory'))
);

CREATE INDEX idx_llm_prompt_assets_version_id ON llm_prompt_assets(version_id);
CREATE INDEX idx_llm_prompt_assets_storage_key ON llm_prompt_assets(storage_key);

-- (variables, tools, response_format columns are defined inline on the llm_prompt_versions table)
COMMENT ON TABLE llm_prompt_assets IS 'Metadata for multimodal assets (images, audio, PDFs). Binary data stored in object storage.';

-- ============================================================================
-- 007_flow_llm_provider_integrations.sql
-- ============================================================================

-- LLM Provider Integrations table
-- Stores metadata about configured AI providers (API keys stored in project_settings)

CREATE TABLE IF NOT EXISTS llm_provider_integrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    provider VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_tested_at TIMESTAMPTZ,
    last_test_status VARCHAR(20) DEFAULT 'never', -- 'success', 'failed', 'never'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Each project can only have one integration per provider
    CONSTRAINT unique_project_provider UNIQUE (project_id, provider)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_llm_provider_integrations_project ON llm_provider_integrations(project_id);

-- Comment on the table
COMMENT ON TABLE llm_provider_integrations IS 'Stores LLM provider integration metadata for Gateway. API keys are stored encrypted in project_settings.';

-- ============================================================================
-- 008_pond_data_warehouse.sql
-- ============================================================================

-- Data Warehouse Schema
-- Stores connector configurations, sync schedules, and job history

-- Function to compute connection config hash for duplicate detection
-- Hashes the relevant connection fields (host, port, database) but not credentials
CREATE OR REPLACE FUNCTION compute_connection_hash(config JSONB, source_type TEXT) 
RETURNS TEXT AS $$
DECLARE
    hash_input TEXT;
BEGIN
    -- Extract connection-identifying fields based on source type
    -- This excludes credentials but includes host/port/database
    CASE source_type
        WHEN 'postgresql', 'mysql', 'sqlserver' THEN
            hash_input := COALESCE(config->>'host', '') || ':' ||
                         COALESCE(config->>'port', '') || ':' ||
                         COALESCE(config->>'database', '');
        WHEN 'clickhouse' THEN
            hash_input := COALESCE(config->>'host', '') || ':' ||
                         COALESCE(config->>'port', '') || ':' ||
                         COALESCE(config->>'database', '');
        WHEN 'mongodb' THEN
            hash_input := COALESCE(config->>'connection_string', 
                         COALESCE(config->>'host', '') || ':' || COALESCE(config->>'port', ''));
        WHEN 'bigquery' THEN
            hash_input := COALESCE(config->>'project_id', '') || ':' ||
                         COALESCE(config->>'dataset', '');
        WHEN 'sqlite' THEN
            hash_input := COALESCE(config->>'path', '');
        WHEN 's3', 'r2' THEN
            hash_input := COALESCE(config->>'bucket', '') || ':' ||
                         COALESCE(config->>'prefix', '');
        ELSE
            -- For other types, use full config minus sensitive fields
            hash_input := config::TEXT;
    END CASE;
    
    RETURN md5(source_type || ':' || hash_input);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Warehouse sources (connected data sources)
CREATE TABLE IF NOT EXISTS warehouse_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,  -- 'postgresql', 'mysql', 'mongodb', etc.
    storage_type TEXT NOT NULL DEFAULT 'object_storage',  -- 'native_clickhouse', 'object_storage', 'external'
    config JSONB NOT NULL,      -- Encrypted credentials and settings
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Storage tier: cold (federated query), warm (Parquet on R2 with indexes), hot (ClickHouse)
    tier TEXT NOT NULL DEFAULT 'cold' 
        CHECK (tier IN ('cold', 'warm', 'hot')),
    
    -- Connection deduplication - hash of connection config for duplicate detection
    connection_config_hash TEXT NOT NULL,
    
    -- Materialization metadata
    warm_at TIMESTAMPTZ,              -- When data was synced to Parquet/R2
    hot_at TIMESTAMPTZ,               -- When data was synced to ClickHouse
    last_sync_at TIMESTAMPTZ,         -- Last successful sync time
    storage_bytes BIGINT DEFAULT 0,   -- Storage used by warm/hot tier data
    
    -- Sync interval for warm/hot tier sources (NULL = manual only)
    -- Fivetran-style intervals: 5m, 15m, 30m, 1h, 6h, 12h, 24h
    sync_interval TEXT CHECK (sync_interval IS NULL OR sync_interval IN ('5m', '15m', '30m', '1h', '6h', '12h', '24h')),
    
    -- Sync scope: full (all data) or time_based (only data older than N days)
    sync_scope TEXT NOT NULL DEFAULT 'full'
        CHECK (sync_scope IN ('full', 'time_based')),
    sync_scope_older_than_days INTEGER,
    
    -- Storage tier lifecycle policy (JSONB)
    -- {"type": "fixed"} = current behavior, tier field determines storage location
    -- {"type": "lifecycle", "transitions": [{"after_days": 30, "tier": "warm"}, {"after_days": 90, "tier": "cold"}]}
    storage_tier_policy JSONB NOT NULL DEFAULT '{"type": "fixed"}',

    -- Checkpoint for resumable sync (LSN, position, timestamp)
    -- Format: {"last_table": "...", "position": "...", "timestamp": 123456}
    sync_checkpoint JSONB,

    -- Whether this source supports CDC/WAL-based change tracking.
    -- Sources without CDC can only use cold tier.
    supports_cdc BOOLEAN DEFAULT true,

    -- Consistency level for synced data: eventual (seconds-minutes delay),
    -- read_after_write (immediate for writer), strong (synchronous)
    consistency_level VARCHAR(20) DEFAULT 'eventual',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_sources_project ON warehouse_sources(project_id);
CREATE INDEX idx_warehouse_sources_tier ON warehouse_sources(project_id, tier);
CREATE UNIQUE INDEX idx_warehouse_sources_unique_connection 
    ON warehouse_sources(project_id, connection_config_hash);

ALTER TABLE warehouse_sources 
ADD CONSTRAINT chk_consistency_level CHECK (consistency_level IN ('eventual', 'read_after_write', 'strong'));

COMMENT ON COLUMN warehouse_sources.tier IS 'cold = federated query at source, warm = Parquet on R2 + local indexes, hot = ClickHouse';
COMMENT ON COLUMN warehouse_sources.connection_config_hash IS 'Hash of connection config for duplicate detection (same connection cannot be added twice)';
COMMENT ON COLUMN warehouse_sources.sync_interval IS 'Fivetran-style sync interval: 5m, 15m, 30m, 1h, 6h, 12h, 24h. NULL = manual sync only';

-- Warehouse tables (tables synced from sources)
CREATE TABLE IF NOT EXISTS warehouse_tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    schema JSONB NOT NULL,        -- Column definitions
    r2_prefix TEXT NOT NULL,       -- Object storage path prefix
    sync_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    incremental_key TEXT,          -- Column for incremental sync
    -- Sync state: pending (being written), committed (ready for queries)
    sync_state VARCHAR(20) NOT NULL DEFAULT 'committed',
    -- Job that created/updated this table (for cleanup tracking)
    job_id UUID,                   -- References warehouse_jobs(id), but jobs table is created later
    -- Detected partition strategy for external Parquet files.
    -- Populated during FST rebuild; used by the query rewriter for partition hints.
    -- JSON: {"HiveStyle": {...}} | {"TimestampBucket": {...}} | {"HashBucket": {...}} | "Flat"
    detected_partition_scheme JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source_id, name)
);

CREATE INDEX idx_warehouse_tables_source ON warehouse_tables(source_id);

-- Sync schedules (persistent cron schedules)
CREATE TABLE IF NOT EXISTS warehouse_sync_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    cron_expression TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_sync_schedules_source ON warehouse_sync_schedules(source_id);
CREATE INDEX idx_warehouse_sync_schedules_enabled ON warehouse_sync_schedules(enabled) WHERE enabled = TRUE;

-- Job queue (sync jobs and other background tasks)
CREATE TABLE IF NOT EXISTS warehouse_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type TEXT NOT NULL,           -- 'sync', 'fst_rebuild', 'schema_snapshot'
    source_id UUID REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    table_name TEXT,                  -- For table-specific syncs
    status TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'completed', 'failed', 'cancelled'
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    error_details JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    -- Locking for distributed workers
    locked_by TEXT,                   -- Worker ID that claimed the job
    locked_at TIMESTAMPTZ,
    lock_expires_at TIMESTAMPTZ,      -- Auto-release if worker dies
    -- Results
    rows_synced BIGINT,
    bytes_written BIGINT,
    files_created INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_jobs_pending ON warehouse_jobs(status, scheduled_at) WHERE status = 'pending';
CREATE INDEX idx_warehouse_jobs_orphaned ON warehouse_jobs(status, lock_expires_at) WHERE status = 'running';
CREATE INDEX idx_warehouse_jobs_source ON warehouse_jobs(source_id);

-- Add foreign key for warehouse_tables.job_id (table created before warehouse_jobs)
ALTER TABLE warehouse_tables 
ADD CONSTRAINT fk_warehouse_tables_job 
FOREIGN KEY (job_id) REFERENCES warehouse_jobs(id) ON DELETE SET NULL;

-- Sync history (completed syncs)
CREATE TABLE IF NOT EXISTS warehouse_syncs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    project_id UUID,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_synced BIGINT NOT NULL DEFAULT 0,
    bytes_written BIGINT NOT NULL DEFAULT 0,
    files_created INT NOT NULL DEFAULT 0,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_syncs_source ON warehouse_syncs(source_id);
CREATE INDEX idx_warehouse_syncs_project ON warehouse_syncs(project_id);
CREATE INDEX idx_warehouse_syncs_table ON warehouse_syncs(source_id, table_name);
CREATE INDEX idx_warehouse_syncs_completed ON warehouse_syncs(completed_at);

-- Sync errors (detailed error logs)
CREATE TABLE IF NOT EXISTS warehouse_sync_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_id UUID REFERENCES warehouse_syncs(id) ON DELETE CASCADE,
    job_id UUID REFERENCES warehouse_jobs(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    suggested_action TEXT,
    retryable BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_sync_errors_sync ON warehouse_sync_errors(sync_id);
CREATE INDEX idx_warehouse_sync_errors_job ON warehouse_sync_errors(job_id);

-- Schema snapshots (for drift detection)
CREATE TABLE IF NOT EXISTS warehouse_schema_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    schema JSONB NOT NULL,  -- Column names, types, nullable
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_schema_snapshots_source ON warehouse_schema_snapshots(source_id, table_name);

-- FST indexes metadata
CREATE TABLE IF NOT EXISTS warehouse_fst_indexes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    index_type TEXT NOT NULL,  -- 'schema', 'column', 'skip', 'history', 'cache', 'join', 'tag', 'fk'
    table_name TEXT,           -- For table-specific indexes
    column_name TEXT,          -- For column indexes
    file_path TEXT NOT NULL,   -- Path in R2 or local disk
    key_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_warehouse_fst_indexes_type ON warehouse_fst_indexes(index_type);
CREATE INDEX idx_warehouse_fst_indexes_active ON warehouse_fst_indexes(is_active) WHERE is_active = TRUE;

-- Usage tracking
CREATE TABLE IF NOT EXISTS warehouse_usage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    query_id UUID,
    bytes_scanned BIGINT NOT NULL,
    files_read INT NOT NULL,
    execution_time_ms INT NOT NULL,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_usage_project ON warehouse_usage(project_id);
CREATE INDEX idx_warehouse_usage_created ON warehouse_usage(created_at);

-- Budget alerts
CREATE TABLE IF NOT EXISTS warehouse_budgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL UNIQUE,
    monthly_bytes_limit BIGINT,  -- NULL = unlimited
    alert_threshold_percent INT DEFAULT 80,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Saved views
CREATE TABLE IF NOT EXISTS warehouse_views (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    name TEXT NOT NULL,
    sql TEXT NOT NULL,
    description TEXT,
    created_by UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

CREATE INDEX idx_warehouse_views_project ON warehouse_views(project_id);

-- Query history
CREATE TABLE IF NOT EXISTS warehouse_query_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    sql TEXT NOT NULL,
    execution_time_ms INT NOT NULL,
    rows_returned BIGINT NOT NULL,
    bytes_scanned BIGINT NOT NULL,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warehouse_query_history_project ON warehouse_query_history(project_id);
CREATE INDEX idx_warehouse_query_history_user ON warehouse_query_history(user_id);
CREATE INDEX idx_warehouse_query_history_executed ON warehouse_query_history(executed_at);

-- ============================================================================
-- 009_pond_warehouse_skip_indexes.sql
-- ============================================================================

-- Skip indexes for data warehouse query optimization
-- Stores FST-based indexes that allow skipping Parquet files during query execution

-- Skip indexes store FST bytes per column per file
-- This allows loading pre-built indexes at query time instead of rebuilding
CREATE TABLE IF NOT EXISTS warehouse_skip_indexes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    table_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,    -- e.g., "2025/01" for date partitions
    file_path TEXT NOT NULL,        -- Path to the Parquet file
    column_name TEXT NOT NULL,      -- Column that is indexed
    values_fst BYTEA NOT NULL,      -- Serialized FST bytes for value lookup
    row_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite unique key: one FST per (project, table, partition, file, column)
    UNIQUE(project_id, table_name, partition_key, file_path, column_name)
);

-- Indexes for efficient loading
-- Primary query: load all indexes for a project
CREATE INDEX idx_warehouse_skip_indexes_project 
    ON warehouse_skip_indexes(project_id);

-- Query by project and table for table-specific operations
CREATE INDEX idx_warehouse_skip_indexes_project_table 
    ON warehouse_skip_indexes(project_id, table_name);

-- Query by project, table, and partition for partition-aware loading
CREATE INDEX idx_warehouse_skip_indexes_project_table_partition 
    ON warehouse_skip_indexes(project_id, table_name, partition_key);

-- Index for cleanup operations (finding old indexes)
CREATE INDEX idx_warehouse_skip_indexes_created 
    ON warehouse_skip_indexes(created_at);

-- (project_id column is defined inline on the warehouse_syncs table)

-- Skip index manifests: lightweight metadata for FST blobs stored in R2.
-- The actual FST data lives in R2 as zstd-compressed blobs; this table tracks
-- which version is current so the local disk cache can stay in sync.
CREATE TABLE IF NOT EXISTS warehouse_skip_index_manifests (
    project_id UUID NOT NULL,
    table_name TEXT NOT NULL,
    r2_key TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    blob_size BIGINT NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    column_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, table_name)
);

CREATE INDEX idx_skip_index_manifests_project
    ON warehouse_skip_index_manifests(project_id);

-- ============================================================================
-- 010_pond_warehouse_table_statistics.sql
-- ============================================================================

-- Warehouse Table Statistics
--
-- Stores statistics for data sources to enable cost-based query planning.
-- Statistics are collected during sync, from database catalogs, or via sampling.

-- Statistics collection method
CREATE TYPE statistics_collection_method AS ENUM (
    'sync',      -- Collected during data sync (Stripe, HubSpot, etc.)
    'sample',    -- Collected by sampling rows from the source
    'metadata',  -- Extracted from file metadata (Parquet row groups)
    'catalog',   -- Queried from database catalog (pg_stats, information_schema)
    'estimate'   -- Rough estimate based on file size
);

-- Table-level statistics
CREATE TABLE warehouse_table_statistics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Row and size statistics
    row_count BIGINT,
    size_bytes BIGINT,
    avg_row_size_bytes INTEGER,
    
    -- File statistics (for Parquet/CSV sources)
    file_count INTEGER,
    
    -- Collection metadata
    collection_method statistics_collection_method NOT NULL,
    sample_rate REAL,  -- For sampled stats: fraction of data sampled (0.0-1.0)
    confidence REAL,   -- Confidence level for estimates (0.0-1.0)
    
    -- Timestamps
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,  -- When stats should be refreshed
    
    -- Constraint: unique per project/source/table
    UNIQUE (project_id, source_name, table_name)
);

-- Column-level statistics
CREATE TABLE warehouse_column_statistics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_stats_id UUID NOT NULL REFERENCES warehouse_table_statistics(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    
    -- Cardinality
    distinct_count BIGINT,
    null_count BIGINT,
    null_fraction REAL,  -- Fraction of nulls (0.0-1.0)
    
    -- Value distribution
    min_value TEXT,  -- Stored as text, interpreted based on column type
    max_value TEXT,
    avg_length INTEGER,  -- Average length for string columns
    
    -- Histogram buckets (for range queries)
    -- Stores up to 100 histogram boundaries as JSON array
    histogram_bounds JSONB,
    
    -- Most common values (for equality predicates)
    -- Format: [{"value": "...", "frequency": 0.05}, ...]
    most_common_values JSONB,
    
    -- Constraint: unique per table_stats/column
    UNIQUE (table_stats_id, column_name)
);

-- Indexes for efficient lookups
CREATE INDEX idx_warehouse_table_stats_project 
    ON warehouse_table_statistics(project_id);

CREATE INDEX idx_warehouse_table_stats_lookup 
    ON warehouse_table_statistics(project_id, source_name, table_name);

CREATE INDEX idx_warehouse_table_stats_expires 
    ON warehouse_table_statistics(expires_at) 
    WHERE expires_at IS NOT NULL;

CREATE INDEX idx_warehouse_column_stats_table 
    ON warehouse_column_statistics(table_stats_id);

-- Trigger to update expires_at based on collection_method
CREATE OR REPLACE FUNCTION set_statistics_expiry()
RETURNS TRIGGER AS $$
BEGIN
    -- Set expiry based on collection method
    NEW.expires_at := CASE NEW.collection_method
        WHEN 'sync' THEN NEW.collected_at + INTERVAL '24 hours'
        WHEN 'sample' THEN NEW.collected_at + INTERVAL '1 hour'
        WHEN 'metadata' THEN NEW.collected_at + INTERVAL '7 days'
        WHEN 'catalog' THEN NEW.collected_at + INTERVAL '12 hours'
        WHEN 'estimate' THEN NEW.collected_at + INTERVAL '1 hour'
    END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_set_statistics_expiry
    BEFORE INSERT OR UPDATE ON warehouse_table_statistics
    FOR EACH ROW
    WHEN (NEW.expires_at IS NULL)
    EXECUTE FUNCTION set_statistics_expiry();

-- ============================================================================
-- 011_pond_unified_catalog.sql
-- ============================================================================

-- Unified Catalog and Metadata
--
-- Provides cross-source schema discovery, lineage tracking, and relationship management.
-- Part of Challenge 7: Unified Catalog implementation.

-- ============================================================================
-- Catalog Entries (Tables across all sources)
-- ============================================================================

CREATE TABLE warehouse_catalog (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source_id UUID REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    source_name VARCHAR(64) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    
    -- Schema as JSONB (TypedColumn array)
    -- Format: [{"name": "id", "data_type": "Int64", "nullable": false, ...}, ...]
    schema JSONB NOT NULL DEFAULT '[]',
    
    -- Metadata
    description TEXT,
    tags JSONB DEFAULT '[]',
    
    -- Freshness tracking
    last_sync_at TIMESTAMPTZ,
    sync_status VARCHAR(20) DEFAULT 'unknown', -- 'synced', 'syncing', 'stale', 'error', 'unknown'
    row_count_estimate BIGINT,
    size_bytes_estimate BIGINT,

    -- Full-text search column configuration
    -- Array of column names marked for substring search via FST indexes
    -- Format: ["column_a", "column_b"]
    fulltext_columns JSONB NOT NULL DEFAULT '[]',
    
    -- Timestamps
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Constraint: unique per project/source/table
    UNIQUE (project_id, source_name, table_name)
);

-- Indexes for catalog lookups
CREATE INDEX idx_warehouse_catalog_project 
    ON warehouse_catalog(project_id);

CREATE INDEX idx_warehouse_catalog_source 
    ON warehouse_catalog(project_id, source_name);

CREATE INDEX idx_warehouse_catalog_lookup 
    ON warehouse_catalog(project_id, source_name, table_name);

CREATE INDEX idx_warehouse_catalog_sync_status 
    ON warehouse_catalog(sync_status) 
    WHERE sync_status IN ('syncing', 'stale', 'error');

-- Full-text search on table names and descriptions
CREATE INDEX idx_warehouse_catalog_search 
    ON warehouse_catalog USING gin(to_tsvector('english', table_name || ' ' || COALESCE(description, '')));

-- ============================================================================
-- Column-level Lineage Tracking
-- ============================================================================

-- Transformation types for lineage edges
CREATE TYPE lineage_transformation_type AS ENUM (
    'direct',      -- Column copied directly (SELECT col FROM ...)
    'derived',     -- Derived via expression (SELECT col * 2 FROM ...)
    'aggregated',  -- Result of aggregation (SELECT SUM(col) FROM ...)
    'joined',      -- Result of join operation
    'filtered',    -- Column used in filter condition
    'unknown'      -- Source unknown or not analyzed
);

-- Discovery methods for lineage
CREATE TYPE lineage_discovery_method AS ENUM (
    'manual',          -- User-defined lineage
    'inferred',        -- Inferred from column names/types
    'query_analysis',  -- Extracted from SQL query parsing
    'sync'             -- Detected during data sync
);

CREATE TABLE warehouse_lineage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    
    -- Target (derived) column - where data flows TO
    target_source VARCHAR(64) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    
    -- Source (origin) column - where data flows FROM
    source_source VARCHAR(64) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    source_column VARCHAR(255) NOT NULL,
    
    -- Transformation info
    transformation_type lineage_transformation_type NOT NULL DEFAULT 'unknown',
    transformation_sql TEXT,  -- SQL expression if available
    
    -- Confidence and metadata
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    discovered_by lineage_discovery_method NOT NULL DEFAULT 'manual',
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Constraint: unique lineage edge
    UNIQUE (project_id, target_source, target_table, target_column, 
            source_source, source_table, source_column)
);

-- Indexes for lineage traversal
CREATE INDEX idx_warehouse_lineage_project 
    ON warehouse_lineage(project_id);

-- Find upstream dependencies (what columns feed into this one)
CREATE INDEX idx_warehouse_lineage_target 
    ON warehouse_lineage(project_id, target_source, target_table, target_column);

-- Find downstream dependencies (what columns depend on this one)
CREATE INDEX idx_warehouse_lineage_source 
    ON warehouse_lineage(project_id, source_source, source_table, source_column);

-- ============================================================================
-- Cross-Source Relationships (Foreign Keys)
-- ============================================================================

-- Relationship types
CREATE TYPE relationship_type AS ENUM (
    'foreign_key',  -- Explicit foreign key from database schema
    'inferred',     -- Inferred from column names and value matching
    'manual'        -- User-defined relationship
);

-- Cardinality types
CREATE TYPE relationship_cardinality AS ENUM (
    'one_to_one',
    'one_to_many',
    'many_to_one',
    'many_to_many',
    'unknown'
);

CREATE TABLE warehouse_relationships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255),
    
    -- "From" side (the referencing table - has the FK column)
    from_source VARCHAR(64) NOT NULL,
    from_table VARCHAR(255) NOT NULL,
    from_columns JSONB NOT NULL,  -- ["customer_id"] or ["col1", "col2"] for composite
    
    -- "To" side (the referenced table - has the PK)
    to_source VARCHAR(64) NOT NULL,
    to_table VARCHAR(255) NOT NULL,
    to_columns JSONB NOT NULL,  -- ["id"] or ["pk1", "pk2"] for composite
    
    -- Relationship metadata
    relationship_type relationship_type NOT NULL DEFAULT 'manual',
    cardinality relationship_cardinality NOT NULL DEFAULT 'unknown',
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    
    -- Validation status
    is_validated BOOLEAN NOT NULL DEFAULT FALSE,
    last_validated_at TIMESTAMPTZ,
    violation_count INTEGER DEFAULT 0,
    sample_violations JSONB,  -- Sample of violating values for debugging
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Constraint: unique relationship definition
    UNIQUE (project_id, from_source, from_table, to_source, to_table, from_columns, to_columns)
);

-- Indexes for relationship lookups
CREATE INDEX idx_warehouse_relationships_project 
    ON warehouse_relationships(project_id);

-- Find relationships from a table
CREATE INDEX idx_warehouse_relationships_from 
    ON warehouse_relationships(project_id, from_source, from_table);

-- Find relationships to a table
CREATE INDEX idx_warehouse_relationships_to 
    ON warehouse_relationships(project_id, to_source, to_table);

-- Find unvalidated relationships
CREATE INDEX idx_warehouse_relationships_unvalidated 
    ON warehouse_relationships(project_id, is_validated) 
    WHERE is_validated = FALSE;

-- ============================================================================
-- Catalog Refresh History
-- ============================================================================

CREATE TABLE warehouse_catalog_refresh_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source_name VARCHAR(64),  -- NULL for full refresh
    
    -- Refresh details
    tables_discovered INTEGER NOT NULL DEFAULT 0,
    tables_updated INTEGER NOT NULL DEFAULT 0,
    tables_removed INTEGER NOT NULL DEFAULT 0,
    relationships_inferred INTEGER NOT NULL DEFAULT 0,
    
    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'pending', 'running', 'completed', 'failed'
    error_message TEXT,
    
    -- Timing
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER
);

CREATE INDEX idx_warehouse_catalog_refresh_project 
    ON warehouse_catalog_refresh_history(project_id);

CREATE INDEX idx_warehouse_catalog_refresh_status 
    ON warehouse_catalog_refresh_history(status) 
    WHERE status IN ('pending', 'running');

-- ============================================================================
-- Triggers for updated_at timestamps
-- ============================================================================

CREATE OR REPLACE FUNCTION update_catalog_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_warehouse_catalog_updated
    BEFORE UPDATE ON warehouse_catalog
    FOR EACH ROW
    EXECUTE FUNCTION update_catalog_timestamp();

CREATE TRIGGER trigger_warehouse_lineage_updated
    BEFORE UPDATE ON warehouse_lineage
    FOR EACH ROW
    EXECUTE FUNCTION update_catalog_timestamp();

CREATE TRIGGER trigger_warehouse_relationships_updated
    BEFORE UPDATE ON warehouse_relationships
    FOR EACH ROW
    EXECUTE FUNCTION update_catalog_timestamp();

-- ============================================================================
-- 013_pond_index_architecture.sql
-- ============================================================================

-- Index Architecture for Warm Tier
-- This migration adds support for time-based partitioning and hybrid indexing
-- (mutable Roaring Bitmap indexes for recent data, frozen FST indexes for old data)

-- Partition tracking table for time-based partitioning in warm tier
-- Each partition represents one day of data for a table
CREATE TABLE IF NOT EXISTS warehouse_partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    partition_date DATE NOT NULL,
    -- Partition state: mutable (recent, uses Roaring) or frozen (old, uses FST)
    state VARCHAR(20) NOT NULL DEFAULT 'mutable',
    -- Sync state: pending (being written), committed (ready for queries)
    sync_state VARCHAR(20) NOT NULL DEFAULT 'committed',
    -- Job that created/updated this partition (for cleanup tracking)
    job_id UUID REFERENCES warehouse_jobs(id) ON DELETE SET NULL,
    -- Path to the Parquet file in R2/S3
    parquet_path VARCHAR(1024),
    -- Row count in this partition
    row_count BIGINT DEFAULT 0,
    -- Size of the Parquet file in bytes
    size_bytes BIGINT DEFAULT 0,
    -- When this partition was last updated (used for freeze scheduling)
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    -- When this partition was frozen (NULL if still mutable)
    frozen_at TIMESTAMPTZ,
    -- Current storage tier for this partition (used by lifecycle worker).
    -- NULL until explicitly set; must be one of 'cold', 'warm', 'hot'.
    current_tier TEXT DEFAULT NULL,
    -- When the lifecycle worker last evaluated this partition's tier
    last_tier_evaluation_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Each source+table can only have one partition per date
    UNIQUE(source_id, table_name, partition_date)
);

-- Index metadata table for tracking indexes on partitions
-- Supports multiple index types per column: roaring, fst, minmax, xor
CREATE TABLE IF NOT EXISTS warehouse_indexes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID NOT NULL REFERENCES warehouse_partitions(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    -- Index type: roaring (mutable), fst (frozen), minmax (skip), xor (bloom-like)
    index_type VARCHAR(50) NOT NULL,
    -- Path to the index file in local storage
    index_path VARCHAR(1024),
    -- Whether this index is valid (false = needs rebuild)
    valid BOOLEAN DEFAULT true,
    -- When this index was last built
    built_at TIMESTAMPTZ,
    -- JSON metadata for the index. For minmax indexes, stores StringMinMaxStats.
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Each partition+column can have multiple index types
    UNIQUE(partition_id, column_name, index_type)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_partitions_source ON warehouse_partitions(source_id);
CREATE INDEX IF NOT EXISTS idx_partitions_state ON warehouse_partitions(state);
CREATE INDEX IF NOT EXISTS idx_partitions_last_updated ON warehouse_partitions(last_updated_at) WHERE state = 'mutable';
CREATE INDEX IF NOT EXISTS idx_indexes_partition ON warehouse_indexes(partition_id);
CREATE INDEX IF NOT EXISTS idx_indexes_valid ON warehouse_indexes(valid) WHERE valid = false;

-- Constraint to ensure valid state values
ALTER TABLE warehouse_partitions 
ADD CONSTRAINT chk_partition_state CHECK (state IN ('mutable', 'frozen'));

-- Constraint to ensure valid sync_state values
ALTER TABLE warehouse_partitions 
ADD CONSTRAINT chk_partition_sync_state CHECK (sync_state IN ('pending', 'committed'));

-- Constraint to ensure valid current_tier values
ALTER TABLE warehouse_partitions
ADD CONSTRAINT chk_partition_tier CHECK (current_tier IS NULL OR current_tier IN ('cold', 'warm', 'hot'));

-- Index for cleanup queries (find pending partitions)
CREATE INDEX IF NOT EXISTS idx_partitions_pending ON warehouse_partitions(source_id, sync_state) 
WHERE sync_state = 'pending';

-- Constraint to ensure valid index types
ALTER TABLE warehouse_indexes 
ADD CONSTRAINT chk_index_type CHECK (index_type IN ('roaring', 'fst', 'minmax', 'xor'));

-- ============================================================================
-- Partition files: tracks individual Parquet files within logical partitions.
-- warehouse_partitions remains as the logical partition (one per source+table+date).
-- Actual file paths live here; warehouse_partitions.parquet_path is deprecated.
-- ============================================================================
CREATE TABLE IF NOT EXISTS warehouse_partition_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID NOT NULL REFERENCES warehouse_partitions(id) ON DELETE CASCADE,
    file_path VARCHAR(1024) NOT NULL,
    sync_version BIGINT NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    -- Which ops this file contains: 'I' (insert-only), 'IU' (inserts+updates),
    -- 'IUD' (all ops), 'D' (tombstone file), etc.
    op_types TEXT NOT NULL DEFAULT 'I',
    job_id UUID REFERENCES warehouse_jobs(id),
    sync_state VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_partition_files_partition ON warehouse_partition_files(partition_id);
CREATE INDEX idx_partition_files_pending ON warehouse_partition_files(sync_state) WHERE sync_state = 'pending';

-- Add primary_key_columns to warehouse_tables for query rewriter dedup
ALTER TABLE warehouse_tables ADD COLUMN IF NOT EXISTS primary_key_columns TEXT[] DEFAULT '{}';

-- ============================================================================
-- 015_pond_apm_dogfood.sql
-- ============================================================================

-- APM Dogfooding: Register Watch's ClickHouse as a Pond data source
--
-- This seeds the warehouse with Watch's ClickHouse instance as a data source,
-- enabling Pond to sync APM spans and logs older than 28 days to warm storage (R2/Parquet).
-- This gives APM data long-term retention beyond Watch's 30-day ClickHouse TTL.
--
-- The ClickHouse host should match your deployment's Watch ClickHouse address.
-- In production, update the host/port in the config JSONB below.

-- Use a DO block so we can check if the source already exists
DO $$
DECLARE
    v_project_id UUID;
BEGIN
    -- Find the first project (or a specific one for dogfooding)
    -- In production, set this to the internal/dogfood project ID
    SELECT id INTO v_project_id
    FROM projects
    ORDER BY created_at ASC
    LIMIT 1;

    -- Only proceed if we have a project
    IF v_project_id IS NOT NULL THEN
        -- Insert the Watch ClickHouse source if it doesn't already exist
        INSERT INTO warehouse_sources (
            project_id,
            name,
            source_type,
            storage_type,
            config,
            enabled,
            tier,
            connection_config_hash,
            sync_scope,
            sync_scope_older_than_days,
            storage_tier_policy
        )
        SELECT
            v_project_id,
            'watch_apm',
            'clickhouse',
            'object_storage',
            jsonb_build_object(
                'host', 'localhost',
                'port', 8123,
                'database', 'default',
                'username', 'default',
                'tables', jsonb_build_array('spans', 'logs')
            ),
            true,
            'cold',
            md5('clickhouse://localhost:8123/default'),
            'time_based',
            28,
            '{"type": "fixed"}'::jsonb
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse_sources
            WHERE project_id = v_project_id
            AND name = 'watch_apm'
        );
    END IF;
END $$;

-- ============================================================================
-- 016_restore_core_dashboard_templates.sql
-- ============================================================================

-- Restore core APM dashboard templates (Services, Kubernetes, Logs)
-- These were accidentally removed during the migration reorganization.

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Services Dashboard',
    'Application Performance Monitoring for HTTP services with request latency, throughput, database queries, and errors',
    'services',
    true,
    1,
    ARRAY['apm', 'http', 'database', 'traces'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""}
        ],
        "tabs": [
            {
                "name": "HTTP",
                "icon": "globe",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Request Error Rate",
                        "x": 0, "y": 0, "w": 6, "h": 3,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"},
                                    {"fn": "count", "alias": "total"},
                                    {"expr": "errors / total * 100", "alias": "error_rate"}
                                ],
                                "where": "span_kind = ''SPAN_KIND_SERVER'' AND span_attributes[''http.route''] != ''''",
                                "groupBy": ["span_attributes[''http.route'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Throughput",
                        "x": 6, "y": 0, "w": 6, "h": 3,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "requests"}],
                                "where": "span_kind = ''SPAN_KIND_SERVER''",
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Request Latency",
                        "x": 0, "y": 3, "w": 8, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "p50"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95"},
                                    {"fn": "quantile", "args": [0.99], "field": "duration", "alias": "p99"}
                                ],
                                "where": "span_kind = ''SPAN_KIND_SERVER''",
                                "interval": "1m"
                            },
                            "unit": "ns"
                        }
                    },
                    {
                        "type": "histogram",
                        "title": "Latency Distribution",
                        "x": 8, "y": 3, "w": 4, "h": 4,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "histogram", "field": "duration", "buckets": 20}],
                                "where": "span_kind = ''SPAN_KIND_SERVER''"
                            },
                            "unit": "ns"
                        }
                    },
                    {
                        "type": "bar",
                        "title": "Top 20 Most Time Consuming Endpoints",
                        "x": 0, "y": 7, "w": 12, "h": 4,
                        "config": {
                            "orientation": "horizontal",
                            "labelField": "endpoint",
                            "valueField": "total_time",
                            "unit": "ns",
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''http.route'']", "alias": "endpoint"},
                                    {"fn": "sum", "field": "duration", "alias": "total_time"},
                                    {"fn": "count", "alias": "requests"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95"}
                                ],
                                "where": "span_kind = ''SPAN_KIND_SERVER'' AND span_attributes[''http.route''] != ''''",
                                "groupBy": ["span_attributes[''http.route'']"],
                                "orderBy": "total_time DESC",
                                "limit": 20
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Endpoints",
                        "x": 0, "y": 11, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "columns": ["endpoint", "req_per_min", "p95_ns", "median_ns", "total_ns", "errors"],
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''http.route'']", "alias": "endpoint"},
                                    {"fn": "count", "alias": "requests"},
                                    {"expr": "requests / 60", "alias": "req_per_min"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_ns"},
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "median_ns"},
                                    {"fn": "sum", "field": "duration", "alias": "total_ns"},
                                    {"fn": "countIf", "args": ["status_code = ''STATUS_CODE_ERROR''"], "alias": "errors"}
                                ],
                                "where": "span_kind = ''SPAN_KIND_SERVER'' AND span_attributes[''http.route''] != ''''",
                                "groupBy": ["span_attributes[''http.route'']"],
                                "orderBy": "total_ns DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Database",
                "icon": "database",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Total Time Consumed per Query",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "sum", "field": "duration", "alias": "total_time"}],
                                "where": "span_attributes[''db.system''] != ''''",
                                "groupBy": ["span_attributes[''db.statement'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Query Throughput",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "queries"}],
                                "where": "span_attributes[''db.system''] != ''''",
                                "groupBy": ["span_attributes[''db.system'']"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Top 20 Most Time Consuming Queries",
                        "x": 0, "y": 4, "w": 12, "h": 6,
                        "config": {
                            "sortable": true,
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_attributes[''db.statement'']", "alias": "statement"},
                                    {"fn": "sum", "field": "duration", "alias": "total_ns"},
                                    {"fn": "count", "alias": "queries"},
                                    {"fn": "quantile", "args": [0.95], "field": "duration", "alias": "p95_ns"},
                                    {"fn": "quantile", "args": [0.5], "field": "duration", "alias": "median_ns"}
                                ],
                                "where": "span_attributes[''db.system''] != ''''",
                                "groupBy": ["span_attributes[''db.statement'']"],
                                "orderBy": "total_ns DESC",
                                "limit": 20
                            }
                        }
                    }
                ]
            },
            {
                "name": "Errors",
                "icon": "alert-triangle",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Error Events per Service",
                        "x": 0, "y": 0, "w": 12, "h": 5,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "errors"}],
                                "where": "status_code = ''STATUS_CODE_ERROR''",
                                "groupBy": ["service_name"],
                                "interval": "1m"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Errors",
                        "x": 0, "y": 5, "w": 12, "h": 5,
                        "config": {
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "timestamp", "alias": "time"},
                                    {"field": "service_name", "alias": "service"},
                                    {"field": "span_name", "alias": "operation"},
                                    {"field": "status_message", "alias": "message"}
                                ],
                                "where": "status_code = ''STATUS_CODE_ERROR''",
                                "orderBy": "timestamp DESC",
                                "limit": 100
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
),
-- Seed: Kubernetes Dashboard (Pods, Nodes, Namespaces tabs)
(
    'Kubernetes Dashboard',
    'Kubernetes infrastructure monitoring with CPU, memory, pod status, and cluster events',
    'infrastructure',
    true,
    2,
    ARRAY['kubernetes', 'k8s', 'infrastructure', 'pods'],
    '{
        "variables": [
            {"name": "namespace", "label": "Namespace", "type": "namespace_select", "default": ""}
        ],
        "tabs": [
            {
                "name": "Pods",
                "icon": "box",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "CPU Usage by Pod",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "cpu"}],
                                "where": "metric_name = ''k8s.pod.cpu.utilization''",
                                "groupBy": ["resource_attributes[''k8s.pod.name'']"],
                                "interval": "1m"
                            },
                            "unit": "percent"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage by Pod",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "memory"}],
                                "where": "metric_name = ''k8s.pod.memory.usage''",
                                "groupBy": ["resource_attributes[''k8s.pod.name'']"],
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "table",
                        "title": "Pods",
                        "x": 0, "y": 4, "w": 12, "h": 5,
                        "config": {
                            "filterable": true,
                            "filterField": "status",
                            "filterOptions": ["Running", "Succeeded", "Pending", "Failed", "All"],
                            "query": {
                                "table": "metrics",
                                "select": [
                                    {"field": "resource_attributes[''k8s.pod.name'']", "alias": "name"},
                                    {"field": "resource_attributes[''k8s.namespace.name'']", "alias": "namespace"},
                                    {"field": "resource_attributes[''k8s.node.name'']", "alias": "node"},
                                    {"field": "resource_attributes[''k8s.pod.phase'']", "alias": "status"}
                                ],
                                "where": "metric_name = ''k8s.pod.phase''",
                                "groupBy": ["resource_attributes[''k8s.pod.name'']", "resource_attributes[''k8s.namespace.name'']", "resource_attributes[''k8s.node.name'']", "resource_attributes[''k8s.pod.phase'']"]
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Latest Kubernetes Warning Events",
                        "x": 0, "y": 9, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [
                                    {"field": "timestamp", "alias": "time"},
                                    {"field": "severity_text", "alias": "severity"},
                                    {"field": "resource_attributes[''k8s.object.kind'']", "alias": "kind"},
                                    {"field": "resource_attributes[''k8s.object.name'']", "alias": "name"},
                                    {"field": "body", "alias": "message"}
                                ],
                                "where": "resource_attributes[''k8s.event.reason''] != '''' AND severity_text = ''Warning''",
                                "orderBy": "timestamp DESC",
                                "limit": 50
                            }
                        }
                    }
                ]
            },
            {
                "name": "Nodes",
                "icon": "server",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "CPU Usage by Node",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "cpu"}],
                                "where": "metric_name = ''k8s.node.cpu.utilization''",
                                "groupBy": ["resource_attributes[''k8s.node.name'']"],
                                "interval": "1m"
                            },
                            "unit": "percent"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage by Node",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "memory"}],
                                "where": "metric_name = ''k8s.node.memory.usage''",
                                "groupBy": ["resource_attributes[''k8s.node.name'']"],
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "table",
                        "title": "Nodes",
                        "x": 0, "y": 4, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [
                                    {"field": "resource_attributes[''k8s.node.name'']", "alias": "node"},
                                    {"field": "resource_attributes[''k8s.node.condition.ready'']", "alias": "status"}
                                ],
                                "where": "metric_name = ''k8s.node.condition.ready''",
                                "groupBy": ["resource_attributes[''k8s.node.name'']", "resource_attributes[''k8s.node.condition.ready'']"]
                            }
                        }
                    }
                ]
            },
            {
                "name": "Namespaces",
                "icon": "folder",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "CPU Usage by Namespace",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "cpu"}],
                                "where": "metric_name = ''k8s.pod.cpu.utilization''",
                                "groupBy": ["resource_attributes[''k8s.namespace.name'']"],
                                "interval": "1m"
                            },
                            "unit": "percent"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Memory Usage by Namespace",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "memory"}],
                                "where": "metric_name = ''k8s.pod.memory.usage''",
                                "groupBy": ["resource_attributes[''k8s.namespace.name'']"],
                                "interval": "1m"
                            },
                            "unit": "bytes"
                        }
                    },
                    {
                        "type": "table",
                        "title": "Namespaces",
                        "x": 0, "y": 4, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [
                                    {"field": "resource_attributes[''k8s.namespace.name'']", "alias": "namespace"},
                                    {"field": "resource_attributes[''k8s.namespace.phase'']", "alias": "phase"}
                                ],
                                "where": "metric_name = ''k8s.namespace.phase''",
                                "groupBy": ["resource_attributes[''k8s.namespace.name'']", "resource_attributes[''k8s.namespace.phase'']"]
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
),
-- Seed: Logs Overview (simpler, no tabs)
(
    'Logs Overview',
    'View log volume, error logs, and log patterns across your services',
    'logs',
    true,
    3,
    ARRAY['logs', 'observability'],
    '{
        "tabs": [
            {
                "name": "Overview",
                "icon": "file-text",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Total Logs (24h)",
                        "x": 0, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [{"fn": "count", "alias": "total"}],
                                "where": "timestamp >= now() - INTERVAL 24 HOUR"
                            }
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Error Logs",
                        "x": 3, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [{"fn": "count", "alias": "errors"}],
                                "where": "severity_text = ''Error'' AND timestamp >= now() - INTERVAL 24 HOUR"
                            },
                            "color": "red"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Warning Logs",
                        "x": 6, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [{"fn": "count", "alias": "warnings"}],
                                "where": "severity_text = ''Warning'' AND timestamp >= now() - INTERVAL 24 HOUR"
                            },
                            "color": "yellow"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Services",
                        "x": 9, "y": 0, "w": 3, "h": 2,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [{"fn": "uniqExact", "field": "service_name", "alias": "services"}],
                                "where": "timestamp >= now() - INTERVAL 24 HOUR"
                            }
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Log Volume by Severity",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "displayMode": "stacked",
                            "query": {
                                "table": "logs",
                                "select": [{"fn": "count", "alias": "logs"}],
                                "groupBy": ["severity_text"],
                                "interval": "5m"
                            }
                        }
                    },
                    {
                        "type": "table",
                        "title": "Recent Logs",
                        "x": 0, "y": 6, "w": 12, "h": 6,
                        "config": {
                            "query": {
                                "table": "logs",
                                "select": [
                                    {"field": "timestamp", "alias": "time"},
                                    {"field": "severity_text", "alias": "severity"},
                                    {"field": "service_name", "alias": "service"},
                                    {"field": "body", "alias": "message"}
                                ],
                                "orderBy": "timestamp DESC",
                                "limit": 100
                            }
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    is_featured = EXCLUDED.is_featured,
    display_order = EXCLUDED.display_order,
    tags = EXCLUDED.tags,
    template_config = EXCLUDED.template_config,
    updated_at = NOW();

-- ============================================================================
-- Rust Runtime dashboard template (Tokio async + Rayon CPU thread pool)
-- ============================================================================

-- Unified runtime dashboard covering both the Tokio async runtime and Rayon
-- CPU thread pools.  Tokio metrics are emitted by `opentelemetry-instrumentation-tokio`;
-- Rayon metrics are emitted by `reiver-sdk`'s InstrumentedThreadPool.
-- Widgets whose metrics are absent gracefully show "no data".

INSERT INTO dashboard_templates (name, description, category, is_featured, display_order, tags, template_config) VALUES
(
    'Rust Runtime',
    'Monitor async and CPU thread pool performance: worker utilization, task scheduling, queue depths, and correlate with request latency',
    'infrastructure',
    true,
    4,
    ARRAY['tokio', 'rayon', 'rust', 'runtime', 'async', 'thread-pool', 'cpu', 'apm'],
    '{
        "variables": [
            {"name": "service", "label": "Service", "type": "service_select", "default": ""}
        ],
        "tabs": [
            {
                "name": "Overview",
                "icon": "activity",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Async Workers",
                        "x": 0, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "workers"}],
                                "where": "metric_name = ''tokio.workers''"
                            },
                            "description": "Number of OS threads powering the async runtime"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Async Active Tasks",
                        "x": 2, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "active_tasks"}],
                                "where": "metric_name = ''tokio.alive_tasks''"
                            },
                            "description": "Futures currently alive. Sustained growth may indicate a task leak"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Async Queue Depth",
                        "x": 4, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "depth"}],
                                "where": "metric_name = ''tokio.global_queue_depth''"
                            },
                            "description": "Tasks waiting in the shared queue. Rising depth signals saturation"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "CPU Pool Threads",
                        "x": 6, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "threads"}],
                                "where": "metric_name = ''rayon.pool.threads''"
                            },
                            "description": "Fixed number of OS threads in the CPU thread pool"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "CPU Tasks Queued",
                        "x": 8, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "queued"}],
                                "where": "metric_name = ''rayon.pool.tasks_queued''"
                            },
                            "color": "yellow",
                            "description": "CPU tasks waiting to start. Rising count means the pool is behind"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "CPU Tasks Panicked",
                        "x": 10, "y": 0, "w": 2, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "panicked"}],
                                "where": "metric_name = ''rayon.pool.tasks_panicked''"
                            },
                            "color": "red",
                            "description": "CPU tasks that panicked. Any nonzero value is a bug"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Execution Time by Worker",
                        "x": 0, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "busy_ms"}],
                                "where": "metric_name = ''tokio.worker.busy_duration''",
                                "groupBy": ["metric_attributes[''tokio.worker.index'']"],
                                "interval": "1m"
                            },
                            "unit": "ms",
                            "description": "Time each worker spent executing futures. Flat at ceiling means the runtime is maxed out"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Tasks Waiting for Work",
                        "x": 6, "y": 2, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "depth"}],
                                "where": "metric_name = ''tokio.global_queue_depth''",
                                "interval": "1m"
                            },
                            "description": "Tasks waiting for a free worker. Rising trend means work arrives faster than it is processed"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Tasks Waiting for a Thread",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "queued"}],
                                "where": "metric_name = ''rayon.pool.tasks_queued''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "description": "CPU work backlog. Sustained growth means the pool needs more threads or less work"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Threads Working",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "active"}],
                                "where": "metric_name = ''rayon.pool.tasks_active''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "description": "CPU threads actively executing tasks. Should stay at or below thread count"
                        }
                    }
                ]
            },
            {
                "name": "Async Workers",
                "icon": "cpu",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "Average Poll Duration by Worker",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "mean_poll_ns"}],
                                "where": "metric_name = ''tokio.worker.mean_poll_time''",
                                "groupBy": ["metric_attributes[''tokio.worker.index'']"],
                                "interval": "1m"
                            },
                            "unit": "ns",
                            "description": "How long each poll of a future takes on average. High values mean slow or blocking futures"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Polls Completed by Worker",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "polls"}],
                                "where": "metric_name = ''tokio.worker.polls''",
                                "groupBy": ["metric_attributes[''tokio.worker.index'']"],
                                "interval": "1m"
                            },
                            "description": "Number of future polls per worker. Uneven distribution signals load imbalance"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Tasks Stolen Between Workers",
                        "x": 0, "y": 4, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "steals"}],
                                "where": "metric_name = ''tokio.worker.task_steals''",
                                "groupBy": ["metric_attributes[''tokio.worker.index'']"],
                                "interval": "1m"
                            },
                            "description": "Tasks taken from another worker''s queue to rebalance load"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Tasks Queued per Worker",
                        "x": 6, "y": 4, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "depth"}],
                                "where": "metric_name = ''tokio.worker.local_queue_depth''",
                                "groupBy": ["metric_attributes[''tokio.worker.index'']"],
                                "interval": "1m"
                            },
                            "description": "Tasks queued locally per worker. Persistent depth means the worker can''t keep up"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Blocking Threads: Total vs Idle",
                        "x": 0, "y": 8, "w": 6, "h": 4,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "total_threads"}],
                                "where": "metric_name = ''tokio.blocking_threads''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "idle_threads"}],
                                "where": "metric_name = ''tokio.idle_blocking_threads''",
                                "interval": "1m"
                            },
                            "description": "Total vs idle blocking threads. Gap between them is active spawn_blocking work"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Cooperative Preemptions",
                        "x": 6, "y": 8, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "yields"}],
                                "where": "metric_name = ''tokio.budget_forced_yields''",
                                "interval": "1m"
                            },
                            "color": "yellow",
                            "description": "Tasks preempted mid-poll to keep the runtime fair. Spikes indicate CPU-heavy futures"
                        }
                    }
                ]
            },
            {
                "name": "CPU Thread Pool",
                "icon": "server",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "CPU Tasks Waiting for a Thread",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "queued"}],
                                "where": "metric_name = ''rayon.pool.tasks_queued''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "description": "Backlog of tasks waiting to start. Rising means the pool is overwhelmed"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Threads Working",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "active"}],
                                "where": "metric_name = ''rayon.pool.tasks_active''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "description": "Threads actively executing. Flat at thread count means fully saturated"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Task Throughput",
                        "x": 0, "y": 4, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "completed"}],
                                "where": "metric_name = ''rayon.pool.tasks_completed''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "description": "Finished CPU tasks per interval"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Task Panics",
                        "x": 6, "y": 4, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "panicked"}],
                                "where": "metric_name = ''rayon.pool.tasks_panicked''",
                                "groupBy": ["metric_attributes[''pool.name'']"],
                                "interval": "1m"
                            },
                            "color": "red",
                            "description": "Tasks that panicked. Should always be zero"
                        }
                    }
                ]
            },
            {
                "name": "IO Driver",
                "icon": "network",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "File Descriptors Registered",
                        "x": 0, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "registrations"}],
                                "where": "metric_name = ''tokio.io_driver.fd_registrations''",
                                "interval": "1m"
                            },
                            "description": "New file descriptors (sockets, files) registered with the IO driver per interval"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "File Descriptors Deregistered",
                        "x": 6, "y": 0, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "deregistrations"}],
                                "where": "metric_name = ''tokio.io_driver.fd_deregistrations''",
                                "interval": "1m"
                            },
                            "description": "File descriptors removed from the IO driver. Should roughly track registrations"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Ready Events Processed",
                        "x": 0, "y": 4, "w": 12, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "ready_events"}],
                                "where": "metric_name = ''tokio.io_driver.fd_readies''",
                                "interval": "1m"
                            },
                            "description": "IO readiness events (readable/writable) the driver delivered to tasks. This is the IO throughput of the runtime"
                        }
                    }
                ]
            },
            {
                "name": "Memory",
                "icon": "database",
                "widgets": [
                    {
                        "type": "stat",
                        "title": "Resident Memory (RSS)",
                        "x": 0, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "rss"}],
                                "where": "metric_name = ''process.memory.rss''"
                            },
                            "description": "Physical memory the process occupies. This is what the OS charges you for"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Virtual Memory",
                        "x": 4, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "vsize"}],
                                "where": "metric_name = ''process.memory.virtual''"
                            },
                            "description": "Total virtual address space. Large values are normal; only physical (RSS) costs real memory"
                        }
                    },
                    {
                        "type": "stat",
                        "title": "Page Faults",
                        "x": 8, "y": 0, "w": 4, "h": 2,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "max", "field": "value", "alias": "faults"}],
                                "where": "metric_name = ''process.memory.page_faults''"
                            },
                            "color": "yellow",
                            "description": "Times the OS loaded memory from disk. High values indicate memory pressure"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "RSS vs Virtual Memory",
                        "x": 0, "y": 2, "w": 12, "h": 4,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "rss"}],
                                "where": "metric_name = ''process.memory.rss''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "vsize"}],
                                "where": "metric_name = ''process.memory.virtual''",
                                "interval": "1m"
                            },
                            "description": "Physical vs virtual memory. A growing gap is normal; a growing RSS is what matters"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Allocator: Allocated vs Resident",
                        "x": 0, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "allocated"}],
                                "where": "metric_name = ''allocator.allocated''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "resident"}],
                                "where": "metric_name = ''allocator.resident''",
                                "interval": "1m"
                            },
                            "description": "Gap between allocated and resident is internal fragmentation plus allocator overhead. Requires jemalloc or mimalloc"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Allocator: Active Pages",
                        "x": 6, "y": 6, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "active"}],
                                "where": "metric_name = ''allocator.active''",
                                "interval": "1m"
                            },
                            "description": "Bytes in active allocator pages. Difference from allocated is internal fragmentation. Requires jemalloc"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Allocator: Virtual Mapped",
                        "x": 0, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "mapped"}],
                                "where": "metric_name = ''allocator.mapped''",
                                "interval": "1m"
                            },
                            "description": "Virtual address space mapped by the allocator. Requires jemalloc"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Allocator: Retained Memory",
                        "x": 6, "y": 10, "w": 6, "h": 4,
                        "config": {
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "retained"}],
                                "where": "metric_name = ''allocator.retained''",
                                "interval": "1m"
                            },
                            "description": "Memory held back from the OS. Growing retention without matching allocation may indicate a memory leak. Requires jemalloc"
                        }
                    }
                ]
            },
            {
                "name": "Correlation",
                "icon": "link",
                "widgets": [
                    {
                        "type": "timeseries",
                        "title": "CPU Work vs Async Execution Time",
                        "x": 0, "y": 0, "w": 12, "h": 5,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "rayon_active"}],
                                "where": "metric_name = ''rayon.pool.tasks_active''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "tokio_busy_ms"}],
                                "where": "metric_name = ''tokio.worker.busy_duration''",
                                "interval": "1m"
                            },
                            "description": "Both rising together means CPU work is contending with the async runtime for CPU time"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "CPU Backlog vs Cooperative Preemptions",
                        "x": 0, "y": 5, "w": 12, "h": 5,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "rayon_queued"}],
                                "where": "metric_name = ''rayon.pool.tasks_queued''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "metrics",
                                "select": [{"fn": "sum", "field": "value", "alias": "budget_yields"}],
                                "where": "metric_name = ''tokio.budget_forced_yields''",
                                "interval": "1m"
                            },
                            "description": "Both rising signals CPU starvation: the CPU pool is consuming resources the async runtime needs"
                        }
                    },
                    {
                        "type": "timeseries",
                        "title": "Async Queue Depth vs Request Throughput",
                        "x": 0, "y": 10, "w": 12, "h": 5,
                        "config": {
                            "dualAxis": true,
                            "query": {
                                "table": "metrics",
                                "select": [{"fn": "avg", "field": "value", "alias": "queue_depth"}],
                                "where": "metric_name = ''tokio.global_queue_depth''",
                                "interval": "1m"
                            },
                            "secondaryQuery": {
                                "table": "spans",
                                "select": [{"fn": "count", "alias": "requests"}],
                                "where": "span_kind = ''SPAN_KIND_SERVER''",
                                "interval": "1m"
                            },
                            "description": "Rising queue under stable throughput means the runtime is saturated and latency is degrading"
                        }
                    },
                    {
                        "type": "table",
                        "title": "Slowest Endpoints",
                        "x": 0, "y": 15, "w": 12, "h": 5,
                        "config": {
                            "sortable": true,
                            "query": {
                                "table": "spans",
                                "select": [
                                    {"field": "span_name", "alias": "operation"},
                                    {"field": "service_name", "alias": "service"},
                                    {"fn": "quantile", "args": [0.99], "field": "duration", "alias": "p99_ns"},
                                    {"fn": "avg", "field": "duration", "alias": "avg_ns"},
                                    {"fn": "count", "alias": "count"}
                                ],
                                "where": "span_kind = ''SPAN_KIND_SERVER''",
                                "groupBy": ["span_name", "service_name"],
                                "orderBy": "p99_ns DESC",
                                "limit": 20
                            },
                            "description": "Endpoints with the worst tail latency, likely impacted by runtime contention"
                        }
                    }
                ]
            }
        ]
    }'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    is_featured = EXCLUDED.is_featured,
    display_order = EXCLUDED.display_order,
    tags = EXCLUDED.tags,
    template_config = EXCLUDED.template_config,
    updated_at = NOW();

-- ============================================================================
-- 023_source_access_log.sql
-- ============================================================================

-- Source access log for access-based tier policies.
-- Tracks when each source is queried so the lifecycle worker
-- can promote/demote sources based on access frequency.

CREATE TABLE IF NOT EXISTS source_access_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    project_id UUID NOT NULL,
    accessed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Composite index for efficient window-based counting per source
CREATE INDEX idx_source_access_source_time ON source_access_log(source_id, accessed_at);

-- Index for cleanup of old rows
CREATE INDEX idx_source_access_accessed_at ON source_access_log(accessed_at);

-- ============================================================================
-- 024_warehouse_pii_findings.sql
-- ============================================================================

-- PII Findings for Warehouse Sources
--
-- Stores per-column PII detection results produced during data sync.
-- Only actual data values are scanned (no column-name heuristics).

CREATE TABLE IF NOT EXISTS warehouse_pii_findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    source_id UUID NOT NULL REFERENCES warehouse_sources(id) ON DELETE CASCADE,
    source_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,

    -- Detected PII types, e.g. ["email", "ssn", "credit_card"]
    pii_types JSONB NOT NULL DEFAULT '[]',

    -- Scan statistics from the latest sync
    total_rows_scanned BIGINT NOT NULL DEFAULT 0,
    rows_with_pii BIGINT NOT NULL DEFAULT 0,

    -- Timestamps
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- User acknowledgement
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'false_positive')),
    acknowledged_by UUID,
    acknowledged_at TIMESTAMPTZ,

    -- One finding per column per source table
    UNIQUE(source_id, table_name, column_name)
);

CREATE INDEX idx_warehouse_pii_findings_project
    ON warehouse_pii_findings(project_id);

CREATE INDEX idx_warehouse_pii_findings_source
    ON warehouse_pii_findings(source_id);

CREATE INDEX idx_warehouse_pii_findings_status
    ON warehouse_pii_findings(project_id, status)
    WHERE status = 'open';

-- (Rayon CPU Thread Pool dashboard merged into 'Rust Runtime' template above)
