//! Strongly-typed domain enums for the gateway layer.
//!
//! These replace raw `String` fields and match-on-string patterns throughout the
//! gateway and API code, providing compile-time safety for known value sets.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// OutputFailureAction
// ---------------------------------------------------------------------------

/// What to do when an LLM response fails output schema validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFailureAction {
    Error,
    Retry,
    RetryThenPassthrough,
    LogOnly,
}

impl Default for OutputFailureAction {
    fn default() -> Self {
        Self::Error
    }
}

impl OutputFailureAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Retry => "retry",
            Self::RetryThenPassthrough => "retry_then_passthrough",
            Self::LogOnly => "log_only",
        }
    }
}

impl fmt::Display for OutputFailureAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OutputFailureAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(Self::Error),
            "retry" => Ok(Self::Retry),
            "retry_then_passthrough" => Ok(Self::RetryThenPassthrough),
            "log_only" => Ok(Self::LogOnly),
            other => Err(format!("unknown output failure action: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// AllocationType
// ---------------------------------------------------------------------------

/// Traffic allocation strategy for prompt rollouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationType {
    Random,
    UserSticky,
    SessionSticky,
}

impl Default for AllocationType {
    fn default() -> Self {
        Self::Random
    }
}

impl AllocationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::UserSticky => "user_sticky",
            Self::SessionSticky => "session_sticky",
        }
    }
}

impl fmt::Display for AllocationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AllocationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "random" => Ok(Self::Random),
            "user_sticky" => Ok(Self::UserSticky),
            "session_sticky" => Ok(Self::SessionSticky),
            other => Err(format!("unknown allocation type: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// GuardrailRule
// ---------------------------------------------------------------------------

/// Machine-readable identifier for a triggered guardrail rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailRule {
    PiiBlocked,
    TokenLimit,
    BlockedInputTopic,
    BlockedOutputTopic,
    PromptInjectionDetected,
    ToolCallBlocked,
    ExfiltrationBlocked,
}

impl GuardrailRule {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PiiBlocked => "pii_blocked",
            Self::TokenLimit => "token_limit",
            Self::BlockedInputTopic => "blocked_input_topic",
            Self::BlockedOutputTopic => "blocked_output_topic",
            Self::PromptInjectionDetected => "prompt_injection_detected",
            Self::ToolCallBlocked => "tool_call_blocked",
            Self::ExfiltrationBlocked => "exfiltration_blocked",
        }
    }
}

// ---------------------------------------------------------------------------
// TrustMode
// ---------------------------------------------------------------------------

/// Controls which message roles are treated as untrusted for injection
/// scanning and spotlighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustMode {
    /// Customer owns the agent; external data arrives via tool results.
    /// Untrusted roles: `[Tool]`.
    Agent,
    /// Platform owns the agent; users are external.
    /// Untrusted roles: `[User, Tool]`.
    Chatbot,
}

impl TrustMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Chatbot => "chatbot",
        }
    }
}

impl fmt::Display for TrustMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for GuardrailRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// RolloutStatus
// ---------------------------------------------------------------------------

/// Status of a prompt rollout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Pending,
    Running,
    Paused,
    Completed,
    RolledBack,
}

impl RolloutStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::RolledBack => "rolled_back",
        }
    }
}

impl fmt::Display for RolloutStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RolloutStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "rolled_back" => Ok(Self::RolledBack),
            other => Err(format!("unknown rollout status: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// RolloutStageStatus
// ---------------------------------------------------------------------------

/// Status of an individual rollout stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStageStatus {
    Pending,
    Active,
    Passed,
    Failed,
}

impl RolloutStageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for RolloutStageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RolloutStageStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown rollout stage status: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// ComparisonStatus
// ---------------------------------------------------------------------------

/// Outcome of comparing target vs baseline metrics during a rollout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonStatus {
    Passing,
    Failing,
    Inconclusive,
}

impl ComparisonStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Passing => "passing",
            Self::Failing => "failing",
            Self::Inconclusive => "inconclusive",
        }
    }
}

impl fmt::Display for ComparisonStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// TestStatus
// ---------------------------------------------------------------------------

/// Result status of a provider integration test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Success,
    Failed,
    Pending,
}

impl Default for TestStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl TestStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Pending => "pending",
        }
    }
}

impl fmt::Display for TestStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TestStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            "pending" => Ok(Self::Pending),
            other => Err(format!("unknown test status: {other}")),
        }
    }
}
