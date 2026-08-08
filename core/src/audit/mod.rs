//! Audit logging for all platform write operations.
//!
//! Every resource mutation (create/update/delete) is recorded as a semantic
//! audit event in ClickHouse via `AuditEventBuilder`. Each event captures:
//! - **WHO** — user or agent (`actor_id`, `caller_type`)
//! - **WHAT** — the action (`event_type`, e.g. `"project.created"`)
//! - **WHERE** — the resource (`resource_type`, `resource_id`)

pub mod clickhouse;

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use uuid::Uuid;

use self::clickhouse::{insert_audit_event, AuditRow};
use crate::clickhouse_db::ClickHousePool;

/// Origin/causation context extracted from HTTP headers.
#[derive(Debug, Clone, Default)]
pub struct AuditOrigin {
    pub origin_type: String,
    pub origin_ref: String,
    pub origin_reason: String,
}

impl AuditOrigin {
    /// Extract origin context from internal service headers.
    /// Falls back to "user" origin when headers are absent (direct UI request).
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let origin_type = headers
            .get("X-Audit-Origin-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("user")
            .to_string();
        let origin_ref = headers
            .get("X-Audit-Origin-Ref")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let origin_reason = headers
            .get("X-Audit-Origin-Reason")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        Self {
            origin_type,
            origin_ref,
            origin_reason,
        }
    }
}

/// Caller identity from internal service headers (`X-Creator-*`).
#[derive(Debug, Clone, Default)]
pub struct AuditCaller {
    pub caller_type: String,
    pub key_label: String,
    pub key_prefix: String,
}

impl AuditCaller {
    /// Extract creator attribution from internal service headers.
    /// When absent, defaults to a direct UI request (`caller_type` = `"user"`).
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let caller_type = headers
            .get("X-Creator-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("user")
            .to_string();
        let key_label = headers
            .get("X-Creator-Key-Label")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let key_prefix = headers
            .get("X-Creator-Key-Prefix")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        Self {
            caller_type,
            key_label,
            key_prefix,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    // SSO Events
    SsoLoginInitiated,
    SsoLoginSuccess,
    SsoLoginFailed,
    SsoLogout,
    SsoConfigCreated,
    SsoConfigUpdated,
    SsoConfigDeleted,

    // SAML Events
    SamlRequestSent,
    SamlResponseReceived,
    SamlResponseVerified,
    SamlResponseFailed,

    // MFA Events
    MfaEnrolled,
    MfaVerified,
    MfaFailed,
    MfaDisabled,
    RecoveryCodeUsed,
    RecoveryCodesGenerated,

    // Session Events
    SessionCreated,
    SessionRevoked,
    SessionExpired,
    SessionsRevokedAll,

    // User / Admin Events
    UserCreated,
    UserUpdated,
    UserProvisioned,
    UserDeprovisioned,
    UserApproved,
    UserDisabled,
    AgentSettingsUpdated,

    // Tier Events
    TierChanged,

    // Certificate Events
    CertificateGenerated,
    CertificateRotated,
    CertificateRevoked,

    // Payment/Billing Events
    PaymentMethodAdded,
    PaymentMethodRemoved,
    PaymentMethodDefaultChanged,
    SubscriptionCreated,
    SubscriptionCanceled,
    SubscriptionUpdated,
    InvoicePaid,
    InvoicePaymentFailed,

    // Budget Events
    BudgetCreated,
    BudgetUpdated,

    // Charge Events
    ChargeGenerated,
    ChargeApproved,
    ChargeRejected,

    // LLM Integration Events
    LlmIntegrationCreated,
    LlmIntegrationUpdated,
    LlmIntegrationDeleted,

    // Secret Slot Events
    SecretSlotCreated,
    SecretSlotFilled,
    SecretSlotConsumed,
    SecretSlotExpired,

    // Project & API Key Events
    ProjectCreated,
    ProjectUpdated,
    ProjectDeleted,
    ApiKeyCreated,
    ApiKeyUpdated,
    ApiKeyDeleted,

    // Dashboard Events
    DashboardCreated,
    DashboardUpdated,
    DashboardDeleted,

    // Organization Membership Events
    InvitationCreated,
    InvitationRevoked,
    MemberRoleUpdated,
    MemberRemoved,

    // SCIM Events
    ScimUserCreated,
    ScimUserUpdated,
    ScimUserDeleted,
    ScimGroupCreated,
    ScimGroupUpdated,
    ScimGroupDeleted,
    ScimGroupMappingCreated,
    ScimGroupMappingDeleted,

    // Provisioning Rule Events
    ProvisioningRuleCreated,
    ProvisioningRuleUpdated,
    ProvisioningRuleDeleted,

    // Prompt Management Events
    PromptConfigCreated,
    PromptConfigUpdated,
    PromptConfigDeleted,
    PromptVersionCreated,

    // Rollout Events
    RolloutCreated,
    RolloutStarted,
    RolloutPaused,
    RolloutPromoted,
    RolloutRolledBack,
    RolloutCompleted,

    // LLM Settings Events
    LlmSettingsUpdated,
    LlmPricingUpdated,

    // Session Profile Events
    SessionProfileCreated,
    SessionProfileUpdated,
    SessionProfileDeleted,

    // Alert Rule Events
    AlertRuleCreated,
    AlertRuleUpdated,
    AlertRuleDeleted,

    // Notification Channel Events
    NotificationChannelCreated,
    NotificationChannelUpdated,
    NotificationChannelDeleted,

    // Maintenance Window Events
    MaintenanceWindowCreated,
    MaintenanceWindowUpdated,
    MaintenanceWindowDeleted,

    // Health Check Events
    HealthCheckCreated,
    HealthCheckUpdated,
    HealthCheckDeleted,

    // Integration Events (cloud/chat — resource_type distinguishes provider)
    IntegrationCreated,
    IntegrationUpdated,
    IntegrationDeleted,

    // Auth Event Integration Events
    AuthEventIntegrationCreated,
    AuthEventIntegrationUpdated,
    AuthEventIntegrationDeleted,

    // Exception Events
    ExceptionGroupUpdated,

    // Prompt Proposal Events
    PromptProposalCreated,
    PromptProposalAccepted,
    PromptProposalDismissed,

    // A2A Access Grant Events
    A2aAccessRequested,
    A2aAccessApproved,
    A2aAccessDenied,
    A2aAccessRevoked,
}

impl AuditEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SsoLoginInitiated => "sso.login.initiated",
            Self::SsoLoginSuccess => "sso.login.success",
            Self::SsoLoginFailed => "sso.login.failed",
            Self::SsoLogout => "sso.logout",
            Self::SsoConfigCreated => "sso.config.created",
            Self::SsoConfigUpdated => "sso.config.updated",
            Self::SsoConfigDeleted => "sso.config.deleted",
            Self::SamlRequestSent => "saml.request.sent",
            Self::SamlResponseReceived => "saml.response.received",
            Self::SamlResponseVerified => "saml.response.verified",
            Self::SamlResponseFailed => "saml.response.failed",
            Self::MfaEnrolled => "mfa.enrolled",
            Self::MfaVerified => "mfa.verified",
            Self::MfaFailed => "mfa.failed",
            Self::MfaDisabled => "mfa.disabled",
            Self::RecoveryCodeUsed => "mfa.recovery_code.used",
            Self::RecoveryCodesGenerated => "mfa.recovery_codes.generated",
            Self::SessionCreated => "session.created",
            Self::SessionRevoked => "session.revoked",
            Self::SessionExpired => "session.expired",
            Self::SessionsRevokedAll => "session.revoked_all",
            Self::UserCreated => "user.created",
            Self::UserUpdated => "user.updated",
            Self::UserProvisioned => "user.provisioned",
            Self::UserDeprovisioned => "user.deprovisioned",
            Self::UserApproved => "user.approved",
            Self::UserDisabled => "user.disabled",
            Self::AgentSettingsUpdated => "agent_settings.updated",
            Self::TierChanged => "tier.changed",
            Self::CertificateGenerated => "certificate.generated",
            Self::CertificateRotated => "certificate.rotated",
            Self::CertificateRevoked => "certificate.revoked",
            Self::PaymentMethodAdded => "payment.method.added",
            Self::PaymentMethodRemoved => "payment.method.removed",
            Self::PaymentMethodDefaultChanged => "payment.method.default_changed",
            Self::SubscriptionCreated => "subscription.created",
            Self::SubscriptionCanceled => "subscription.canceled",
            Self::SubscriptionUpdated => "subscription.updated",
            Self::InvoicePaid => "invoice.paid",
            Self::InvoicePaymentFailed => "invoice.payment_failed",
            Self::BudgetCreated => "budget.created",
            Self::BudgetUpdated => "budget.updated",
            Self::ChargeGenerated => "charge.generated",
            Self::ChargeApproved => "charge.approved",
            Self::ChargeRejected => "charge.rejected",
            Self::LlmIntegrationCreated => "llm.integration.created",
            Self::LlmIntegrationUpdated => "llm.integration.updated",
            Self::LlmIntegrationDeleted => "llm.integration.deleted",
            Self::SecretSlotCreated => "secret_slot.created",
            Self::SecretSlotFilled => "secret_slot.filled",
            Self::SecretSlotConsumed => "secret_slot.consumed",
            Self::SecretSlotExpired => "secret_slot.expired",
            Self::ProjectCreated => "project.created",
            Self::ProjectUpdated => "project.updated",
            Self::ProjectDeleted => "project.deleted",
            Self::ApiKeyCreated => "api_key.created",
            Self::ApiKeyUpdated => "api_key.updated",
            Self::ApiKeyDeleted => "api_key.deleted",
            Self::DashboardCreated => "dashboard.created",
            Self::DashboardUpdated => "dashboard.updated",
            Self::DashboardDeleted => "dashboard.deleted",
            Self::InvitationCreated => "invitation.created",
            Self::InvitationRevoked => "invitation.revoked",
            Self::MemberRoleUpdated => "member.role_updated",
            Self::MemberRemoved => "member.removed",
            Self::ScimUserCreated => "scim.user.created",
            Self::ScimUserUpdated => "scim.user.updated",
            Self::ScimUserDeleted => "scim.user.deleted",
            Self::ScimGroupCreated => "scim.group.created",
            Self::ScimGroupUpdated => "scim.group.updated",
            Self::ScimGroupDeleted => "scim.group.deleted",
            Self::ScimGroupMappingCreated => "scim.group_mapping.created",
            Self::ScimGroupMappingDeleted => "scim.group_mapping.deleted",
            Self::ProvisioningRuleCreated => "provisioning_rule.created",
            Self::ProvisioningRuleUpdated => "provisioning_rule.updated",
            Self::ProvisioningRuleDeleted => "provisioning_rule.deleted",
            Self::PromptConfigCreated => "prompt_config.created",
            Self::PromptConfigUpdated => "prompt_config.updated",
            Self::PromptConfigDeleted => "prompt_config.deleted",
            Self::PromptVersionCreated => "prompt_version.created",
            Self::RolloutCreated => "rollout.created",
            Self::RolloutStarted => "rollout.started",
            Self::RolloutPaused => "rollout.paused",
            Self::RolloutPromoted => "rollout.promoted",
            Self::RolloutRolledBack => "rollout.rolled_back",
            Self::RolloutCompleted => "rollout.completed",
            Self::LlmSettingsUpdated => "llm_settings.updated",
            Self::LlmPricingUpdated => "llm_pricing.updated",
            Self::SessionProfileCreated => "session_profile.created",
            Self::SessionProfileUpdated => "session_profile.updated",
            Self::SessionProfileDeleted => "session_profile.deleted",
            Self::AlertRuleCreated => "alert_rule.created",
            Self::AlertRuleUpdated => "alert_rule.updated",
            Self::AlertRuleDeleted => "alert_rule.deleted",
            Self::NotificationChannelCreated => "notification_channel.created",
            Self::NotificationChannelUpdated => "notification_channel.updated",
            Self::NotificationChannelDeleted => "notification_channel.deleted",
            Self::MaintenanceWindowCreated => "maintenance_window.created",
            Self::MaintenanceWindowUpdated => "maintenance_window.updated",
            Self::MaintenanceWindowDeleted => "maintenance_window.deleted",
            Self::HealthCheckCreated => "health_check.created",
            Self::HealthCheckUpdated => "health_check.updated",
            Self::HealthCheckDeleted => "health_check.deleted",
            Self::IntegrationCreated => "integration.created",
            Self::IntegrationUpdated => "integration.updated",
            Self::IntegrationDeleted => "integration.deleted",
            Self::AuthEventIntegrationCreated => "auth_event_integration.created",
            Self::AuthEventIntegrationUpdated => "auth_event_integration.updated",
            Self::AuthEventIntegrationDeleted => "auth_event_integration.deleted",
            Self::ExceptionGroupUpdated => "exception_group.updated",
            Self::PromptProposalCreated => "prompt.proposal.created",
            Self::PromptProposalAccepted => "prompt.proposal.accepted",
            Self::PromptProposalDismissed => "prompt.proposal.dismissed",
            Self::A2aAccessRequested => "a2a.access.requested",
            Self::A2aAccessApproved => "a2a.access.approved",
            Self::A2aAccessDenied => "a2a.access.denied",
            Self::A2aAccessRevoked => "a2a.access.revoked",
        }
    }
}

/// Builder for creating audit events. Writes to ClickHouse.
pub struct AuditEventBuilder {
    event_type: AuditEventType,
    organization_id: Option<Uuid>,
    user_id: Option<Uuid>,
    actor_id: Option<Uuid>,
    ip_address: Option<String>,
    user_agent: Option<String>,
    resource_type: Option<String>,
    resource_id: Option<Uuid>,
    project_id: Option<String>,
    details: Option<serde_json::Value>,
    success: bool,
    error_message: Option<String>,
    origin_type: Option<String>,
    origin_ref: Option<String>,
    origin_reason: Option<String>,
    /// When set (e.g. from `X-Creator-Type`), overrides inferred user/system caller_type.
    caller_type_override: Option<String>,
    caller_key_label: Option<String>,
    caller_key_prefix: Option<String>,
}

impl AuditEventBuilder {
    pub fn new(event_type: AuditEventType) -> Self {
        Self {
            event_type,
            organization_id: None,
            user_id: None,
            actor_id: None,
            ip_address: None,
            user_agent: None,
            resource_type: None,
            resource_id: None,
            project_id: None,
            details: None,
            success: true,
            error_message: None,
            origin_type: None,
            origin_ref: None,
            origin_reason: None,
            caller_type_override: None,
            caller_key_label: None,
            caller_key_prefix: None,
        }
    }

    pub fn organization(mut self, id: Uuid) -> Self {
        self.organization_id = Some(id);
        self
    }

    pub fn user(mut self, id: Uuid) -> Self {
        self.user_id = Some(id);
        self
    }

    pub fn actor(mut self, id: Uuid) -> Self {
        self.actor_id = Some(id);
        self
    }

    pub fn ip(mut self, ip: &str) -> Self {
        self.ip_address = Some(ip.to_string());
        self
    }

    pub fn user_agent(mut self, ua: &str) -> Self {
        self.user_agent = Some(ua.to_string());
        self
    }

    pub fn resource(mut self, resource_type: &str, resource_id: Uuid) -> Self {
        self.resource_type = Some(resource_type.to_string());
        self.resource_id = Some(resource_id);
        self
    }

    pub fn project(mut self, id: &str) -> Self {
        self.project_id = Some(id.to_string());
        self
    }

    pub fn details<T: Serialize>(mut self, details: T) -> Self {
        self.details = serde_json::to_value(details).ok();
        self
    }

    pub fn success(mut self) -> Self {
        self.success = true;
        self.error_message = None;
        self
    }

    pub fn failure(mut self, message: &str) -> Self {
        self.success = false;
        self.error_message = Some(message.to_string());
        self
    }

    pub fn origin(mut self, origin_type: &str, origin_ref: &str, origin_reason: &str) -> Self {
        self.origin_type = Some(origin_type.to_string());
        self.origin_ref = Some(origin_ref.to_string());
        self.origin_reason = Some(origin_reason.to_string());
        self
    }

    /// Caller attribution from internal headers (`AuditCaller::from_headers`).
    pub fn caller(mut self, caller_type: &str, key_label: &str, key_prefix: &str) -> Self {
        self.caller_type_override = Some(caller_type.to_string());
        self.caller_key_label = Some(key_label.to_string());
        self.caller_key_prefix = Some(key_prefix.to_string());
        self
    }

    /// Log the audit event to ClickHouse. Fire-and-forget via tokio::spawn.
    pub async fn log(self, ch: &ClickHousePool) {
        let event_type_str = self.event_type.as_str();

        let caller_id = self.actor_id.or(self.user_id);

        let caller_type = self.caller_type_override.clone().unwrap_or_else(|| {
            if caller_id.is_some() {
                "user".to_string()
            } else {
                "system".to_string()
            }
        });

        if self.success {
            info!(
                event_type = event_type_str,
                organization_id = ?self.organization_id,
                actor_id = ?caller_id,
                resource_type = ?self.resource_type,
                resource_id = ?self.resource_id,
                "Audit event"
            );
        } else {
            error!(
                event_type = event_type_str,
                organization_id = ?self.organization_id,
                actor_id = ?caller_id,
                error = ?self.error_message,
                "Audit event (failure)"
            );
        }

        let row = AuditRow {
            event_type: event_type_str.to_string(),
            project_id: self.project_id.unwrap_or_default(),
            organization_id: self
                .organization_id
                .map(|u| u.to_string())
                .unwrap_or_default(),
            actor_id: caller_id.map(|u| u.to_string()).unwrap_or_default(),
            caller_type,
            caller_user_id: self.user_id.map(|u| u.to_string()).unwrap_or_default(),
            caller_key_label: self.caller_key_label.unwrap_or_default(),
            caller_key_prefix: self.caller_key_prefix.unwrap_or_default(),
            ip_address: self.ip_address.unwrap_or_default(),
            user_agent: self.user_agent.unwrap_or_default(),
            resource_type: self.resource_type.unwrap_or_default(),
            resource_id: self.resource_id.map(|u| u.to_string()).unwrap_or_default(),
            details: self.details.map(|v| v.to_string()).unwrap_or_default(),
            success: if self.success { 1 } else { 0 },
            error_message: self.error_message.unwrap_or_default(),
            origin_type: self.origin_type.unwrap_or_default(),
            origin_ref: self.origin_ref.unwrap_or_default(),
            origin_reason: self.origin_reason.unwrap_or_default(),
            ..AuditRow::default()
        };

        let ch = ch.clone();
        tokio::spawn(async move {
            insert_audit_event(&ch, row).await;
        });
    }
}
