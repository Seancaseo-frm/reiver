//! Embedded documentation served as MCP resources.
//!
//! Agent-framed versions of the platform docs — same content, but with clear
//! contextual labels so agents understand what each piece of information is for.
//! REST/SDK sections are labeled as "Application Integration" and MCP operations
//! as "Platform Management (MCP)."

pub struct DocPage {
    pub uri: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub content: &'static str,
}

/// Initialization guidance shared by stdio and Streamable HTTP transports.
/// Keep this concise because some MCP clients truncate server instructions.
pub const SERVER_INSTRUCTIONS: &str =
    "Reiver MCP. Before onboarding or changing an application, read agent://onboarding. Select the smallest Flow, Watch, or Complete track and use its definition of done. Read gateway_settings.agent_soul when get and llm:read are available, then reconcile it with the application. Token scopes are the technical boundary; the owner's assignment is the behavioural boundary. Read fresh state before writes, update only intended fields, preserve unrelated settings, and never expose credentials.";

macro_rules! doc_page {
    ($uri:expr, $name:expr, $desc:expr, $path:expr) => {
        DocPage {
            uri: $uri,
            name: $name,
            description: $desc,
            content: include_str!($path),
        }
    };
}

pub static ALL_DOCS: &[DocPage] = &[
    doc_page!(
        "agent://onboarding",
        "Reiver — Application Onboarding",
        "Read first: track selection, business context, credential boundaries, safe writes, integration workflows, and evidence-based acceptance checks",
        "../agent-docs/onboarding.md"
    ),
    doc_page!(
        "agent://overview",
        "Reiver Overview",
        "Platform summary, common workflows, and the distinction between application integration and platform management",
        "../agent-docs/overview.md"
    ),
    // ── Flow ──
    doc_page!(
        "agent://flow/getting-started",
        "Flow — Getting Started",
        "Application integration: connecting to the LLM gateway with code examples (Python, Node.js, cURL)",
        "../agent-docs/flow-getting-started.md"
    ),
    doc_page!(
        "agent://flow/prompt-management",
        "Flow — Prompt Management",
        "Prompt configs, versions, rollouts, template variables — application integration patterns and MCP management workflows",
        "../agent-docs/flow-prompt-management.md"
    ),
    doc_page!(
        "agent://flow/features",
        "Flow — Features",
        "Routing, caching, guardrails, PII masking, session budgets, output contracts, extended thinking, multimodal",
        "../agent-docs/flow-features.md"
    ),
    doc_page!(
        "agent://flow/routing",
        "Flow — Routing",
        "Multi-provider routing: fallback chains, provider preferences, latency-based sorting, auto mode",
        "../agent-docs/flow-routing.md"
    ),
    doc_page!(
        "agent://flow/session-telemetry",
        "Flow — Session Telemetry",
        "Correlating OTel spans and logs with LLM sessions — application tagging patterns and MCP session queries",
        "../agent-docs/flow-session-telemetry.md"
    ),
    doc_page!(
        "agent://flow/models",
        "Flow — Live Model Discovery",
        "Live project model catalogue, Reiver-owned auto routing, and rules for explicit application pins",
        "../agent-docs/flow-models.md"
    ),
    doc_page!(
        "agent://flow/api-reference",
        "Flow — Application Gateway Endpoint",
        "OpenAI-compatible chat completions endpoint that applications send requests to — request/response format, headers, error codes",
        "../agent-docs/flow-api-reference.md"
    ),
    doc_page!(
        "agent://flow/management-api",
        "Flow — Management API",
        "REST endpoints and MCP equivalents for sessions, prompts, rollouts, playground, integrations, and gateway settings",
        "../agent-docs/flow-management-api.md"
    ),
    // ── Watch ──
    doc_page!(
        "agent://watch/overview",
        "Watch — APM Overview",
        "Application integration (OTel ingest) and platform management (traces, logs, dashboards, alerts) via MCP",
        "../agent-docs/watch-overview.md"
    ),
    // ── Tools ──
    doc_page!(
        "agent://tools",
        "Available Tools",
        "Reference for all 5 MCP tools: search, get, list, analyze, execute",
        "../agent-docs/agent-tools.md"
    ),
    // ── SDKs ──
    doc_page!(
        "agent://sdks",
        "Application Libraries (SDKs)",
        "Client libraries for Python, Rust, Unity, and Unreal Engine — for integrating Reiver into applications",
        "../agent-docs/sdks.md"
    ),
];

/// Look up a doc page by its URI.
pub fn find_doc(uri: &str) -> Option<&'static DocPage> {
    ALL_DOCS.iter().find(|d| d.uri == uri)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onboarding_is_the_first_resource() {
        assert_eq!(
            ALL_DOCS.first().map(|doc| doc.uri),
            Some("agent://onboarding")
        );
    }

    #[test]
    fn onboarding_contract_contains_required_boundaries() {
        let content = find_doc("agent://onboarding").unwrap().content;
        for marker in [
            "Flow + Prompt Hub",
            "Watch",
            "Complete Reiver",
            "gateway_settings",
            "agent_soul",
            "Session and Identity Contract",
            "session_labels: []",
            "session_profiles: []",
            "owner's assignment",
            "model_catalog",
            "default_fallback_models",
            "model: \"auto\"",
        ] {
            assert!(
                content.contains(marker),
                "missing onboarding marker: {marker}"
            );
        }
    }

    #[test]
    fn server_instructions_are_compact_and_point_to_onboarding() {
        assert!(SERVER_INSTRUCTIONS.len() <= 512);
        assert!(SERVER_INSTRUCTIONS.contains("agent://onboarding"));
        assert!(SERVER_INSTRUCTIONS.contains("preserve unrelated settings"));
    }

    #[test]
    fn flow_onboarding_docs_require_live_model_discovery() {
        for uri in [
            "agent://onboarding",
            "agent://flow/getting-started",
            "agent://flow/models",
            "agent://flow/prompt-management",
            "agent://flow/routing",
            "agent://flow/api-reference",
        ] {
            let content = find_doc(uri).unwrap().content;
            assert!(
                content.contains("model_catalog"),
                "{uri} should use the live model catalogue"
            );
        }

        for uri in [
            "agent://onboarding",
            "agent://flow/getting-started",
            "agent://flow/models",
            "agent://flow/routing",
        ] {
            let content = find_doc(uri).unwrap().content;
            assert!(
                content.contains("model: \"auto\"") || content.contains("model=\"auto\""),
                "{uri} should teach Reiver-owned auto routing"
            );
        }
    }

    #[test]
    fn flow_onboarding_docs_do_not_pin_stale_claude_models() {
        let stale_ids = [
            "claude-3-5-sonnet",
            "claude-sonnet-4-6",
            "claude-opus-4-6",
            "claude-opus-5-fast",
        ];
        for uri in [
            "agent://onboarding",
            "agent://flow/getting-started",
            "agent://flow/models",
            "agent://flow/prompt-management",
            "agent://flow/routing",
            "agent://flow/api-reference",
            "agent://flow/session-telemetry",
        ] {
            let content = find_doc(uri).unwrap().content;
            for stale_id in stale_ids {
                assert!(
                    !content.contains(stale_id),
                    "{uri} should not pin stale model ID {stale_id}"
                );
            }
        }
    }
}
