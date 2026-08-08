//! OTel GenAI semantic convention metrics for the Flow service.
//!
//! Implements metrics defined in the OpenTelemetry GenAI specification:
//! - `gen_ai.client.operation.duration` (histogram, seconds)
//! - `gen_ai.client.token.usage` (counter)
//!
//! Additional custom metrics use the `gen_ai.` namespace for consistency.

use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};

pub struct FlowMetrics {
    /// gen_ai.client.operation.duration - histogram in seconds.
    /// Differentiate gateway vs agent via `gen_ai.operation.name` attribute.
    pub operation_duration: Histogram<f64>,

    /// gen_ai.client.token.usage - counter.
    /// Use `gen_ai.token.type` = "input" | "output" attribute.
    pub token_usage: Counter<u64>,

    pub cache_hit: Counter<u64>,
    pub cache_miss: Counter<u64>,
    pub provider_error: Counter<u64>,
    pub fallback_used: Counter<u64>,
    pub guardrail_blocked: Counter<u64>,
    pub active_streams: UpDownCounter<i64>,

    /// Tracks turns per agent conversation (histogram).
    pub agent_turns: Histogram<f64>,
    /// Tool calls made by the agent (counter).
    pub agent_tool_calls: Counter<u64>,
}

impl FlowMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("reiver-flow");

        Self {
            operation_duration: meter
                .f64_histogram("gen_ai.client.operation.duration")
                .with_description("Duration of GenAI operations")
                .with_unit("s")
                .build(),
            token_usage: meter
                .u64_counter("gen_ai.client.token.usage")
                .with_description("Measures number of input and output tokens used")
                .with_unit("{token}")
                .build(),
            cache_hit: meter
                .u64_counter("gen_ai.client.cache.hit")
                .with_description("Cache hits for requests")
                .build(),
            cache_miss: meter
                .u64_counter("gen_ai.client.cache.miss")
                .with_description("Cache misses for requests")
                .build(),
            provider_error: meter
                .u64_counter("gen_ai.client.error")
                .with_description("Provider errors (HTTP, timeout, rate limit)")
                .build(),
            fallback_used: meter
                .u64_counter("gen_ai.client.fallback.used")
                .with_description("Requests that used a fallback provider")
                .build(),
            guardrail_blocked: meter
                .u64_counter("gen_ai.client.guardrail.blocked")
                .with_description("Requests blocked by input guardrails")
                .build(),
            active_streams: meter
                .i64_up_down_counter("gen_ai.client.active_streams")
                .with_description("Currently active streaming responses")
                .build(),
            agent_turns: meter
                .f64_histogram("gen_ai.client.agent.turns")
                .with_description("Number of LLM turns per agent conversation")
                .build(),
            agent_tool_calls: meter
                .u64_counter("gen_ai.client.agent.tool_calls")
                .with_description("Tool calls made by agents")
                .build(),
        }
    }
}
