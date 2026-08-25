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

/// Initialization guidance shared by the stdio and Streamable HTTP transports.
/// Keep this concise: some MCP clients truncate server instructions.
pub const SERVER_INSTRUCTIONS: &str = "Reiver MCP. First read agent://onboarding and gateway_settings.agent_soul. Honour the selected Flow, Watch, or Complete track and definition of done. Reuse business context. Confirm the Session and Identity Contract before correlation or labels; ask only about material gaps. Scopes are the hard boundary; the owner's assignment defines autonomy. Within authority, act without repeated approval. Verify traces, logs, metrics independently. Gateway and OTLP require SDK keys. Never expose credentials.";

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
        "Reiver — Verified Application Onboarding",
        "Read first: business discovery, delegated autonomy, credential boundaries, integration workflow, activation and acceptance checks",
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
        "Flow — Session and Identity Contract",
        "Choosing business session boundaries and stable pseudonymous users, then correlating Flow and Watch with MCP-verifiable identifiers",
        "../agent-docs/flow-session-telemetry.md"
    ),
    doc_page!(
        "agent://flow/models",
        "Flow — Supported Models",
        "Model identifiers, routing prefixes, and provider mapping for OpenAI, Anthropic, Gemini, Bedrock, DeepSeek, Theta",
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
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn onboarding_is_first_and_all_resource_uris_are_unique() {
        assert_eq!(
            ALL_DOCS.first().map(|doc| doc.uri),
            Some("agent://onboarding")
        );

        let mut uris = HashSet::new();
        for doc in ALL_DOCS {
            assert!(
                uris.insert(doc.uri),
                "duplicate MCP resource URI: {}",
                doc.uri
            );
            assert!(
                !doc.content.trim().is_empty(),
                "empty MCP resource: {}",
                doc.uri
            );
        }
    }

    #[test]
    fn initialization_instructions_are_concise_and_track_aware() {
        assert!(SERVER_INSTRUCTIONS.len() <= 512);
        assert!(SERVER_INSTRUCTIONS.contains("agent://onboarding"));
        assert!(SERVER_INSTRUCTIONS.contains("business context"));
        assert!(SERVER_INSTRUCTIONS.contains("hard boundary"));
        assert!(SERVER_INSTRUCTIONS.contains("selected Flow, Watch, or Complete track"));
        assert!(SERVER_INSTRUCTIONS.contains("Session and Identity Contract"));
    }
}
