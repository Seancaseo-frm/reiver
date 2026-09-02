use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
#[allow(unused_imports)]
use rmcp::model::{CallToolResult, Content, ErrorCode, Tool};
use schemars::schema_for;

use crate::action::{ActionContext, PlatformAction};

/// Flatten a JSON Schema that uses `oneOf`/`anyOf` (from schemars tagged enums)
/// into a plain object schema compatible with LLM tool-calling APIs.
///
/// For an internally-tagged enum like `#[serde(tag = "analysis")]`, schemars
/// produces `{ oneOf: [ { type: "object", properties: { analysis: { const: "..." }, ...fields } }, ... ] }`.
/// This function detects the tag field, collects all variant enum values, merges
/// all variant properties into a single flat properties map, and produces:
/// `{ type: "object", properties: { analysis: { type: "string", enum: [...] }, ...all_fields }, required: ["analysis"] }`.
fn flatten_oneof_schema(
    mut schema: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let variants_key = if schema.contains_key("oneOf") {
        "oneOf"
    } else if schema.contains_key("anyOf") {
        "anyOf"
    } else {
        return schema;
    };

    let variants = match schema.get(variants_key).and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr.clone(),
        _ => return schema,
    };

    // Resolve $ref references against the definitions map
    let definitions = schema.get("definitions").cloned().unwrap_or_default();
    let resolve =
        |variant: &serde_json::Value| -> Option<serde_json::Map<String, serde_json::Value>> {
            if let Some(ref_path) = variant.get("$ref").and_then(|r| r.as_str()) {
                let def_name = ref_path.rsplit('/').next()?;
                definitions.get(def_name)?.as_object().cloned()
            } else {
                variant.as_object().cloned()
            }
        };

    // Find the tag field: a property whose value uses `const` or single-value `enum`
    // and appears in every variant.
    let mut tag_field: Option<String> = None;
    let mut tag_values: Vec<String> = Vec::new();
    let mut merged_properties = serde_json::Map::new();

    let resolved: Vec<_> = variants.iter().filter_map(|v| resolve(v)).collect();
    if resolved.is_empty() {
        return schema;
    }

    // Detect the tag field from the first variant
    if let Some(first_props) = resolved[0].get("properties").and_then(|p| p.as_object()) {
        for (key, val) in first_props {
            let is_tag = val.get("const").is_some()
                || val
                    .get("enum")
                    .and_then(|e| e.as_array())
                    .map_or(false, |a| a.len() == 1);
            if is_tag {
                // Verify this field is a tag in all variants
                let all_have_it = resolved.iter().all(|v| {
                    v.get("properties")
                        .and_then(|p| p.get(key))
                        .map_or(false, |pv| {
                            pv.get("const").is_some()
                                || pv
                                    .get("enum")
                                    .and_then(|e| e.as_array())
                                    .map_or(false, |a| a.len() == 1)
                        })
                });
                if all_have_it {
                    tag_field = Some(key.clone());
                    break;
                }
            }
        }
    }

    let tag_field = match tag_field {
        Some(t) => t,
        None => return schema,
    };

    // Collect tag values and merge all properties
    for variant in &resolved {
        if let Some(props) = variant.get("properties").and_then(|p| p.as_object()) {
            for (key, val) in props {
                if key == &tag_field {
                    if let Some(c) = val.get("const").and_then(|c| c.as_str()) {
                        tag_values.push(c.to_string());
                    } else if let Some(arr) = val.get("enum").and_then(|e| e.as_array()) {
                        if let Some(c) = arr.first().and_then(|v| v.as_str()) {
                            tag_values.push(c.to_string());
                        }
                    }
                } else {
                    merged_properties
                        .entry(key.clone())
                        .or_insert_with(|| val.clone());
                }
            }
        }
    }

    // Build the tag property as a string enum
    merged_properties.insert(
        tag_field.clone(),
        serde_json::json!({
            "type": "string",
            "description": format!("Discriminator — one of: {}", tag_values.join(", ")),
            "enum": tag_values,
        }),
    );

    // Remove oneOf/anyOf and set the flattened structure
    schema.remove("oneOf");
    schema.remove("anyOf");
    schema.insert("type".to_string(), serde_json::json!("object"));
    schema.insert(
        "properties".to_string(),
        serde_json::Value::Object(merged_properties),
    );
    schema.insert("required".to_string(), serde_json::json!([tag_field]));

    schema
}

/// Type-erased handler that hides the concrete Input/Output types.
#[async_trait]
trait ErasedHandler: Send + Sync {
    async fn call(
        &self,
        ctx: &ActionContext,
        input: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value>;
}

/// Wraps a concrete `PlatformAction` and performs Value <-> typed conversion.
struct TypedHandler<A: PlatformAction>(A);

#[async_trait]
impl<A: PlatformAction> ErasedHandler for TypedHandler<A> {
    async fn call(
        &self,
        ctx: &ActionContext,
        input: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let typed_input: A::Input = serde_json::from_value(input)?;
        let output = self.0.execute(ctx, typed_input).await?;
        Ok(serde_json::to_value(output)?)
    }
}

struct RegisteredAction {
    tool: Tool,
    required_scope: String,
    handler: Box<dyn ErasedHandler>,
}

/// Holds all registered actions and dispatches MCP tool calls.
pub struct ActionRegistry {
    actions: HashMap<&'static str, RegisteredAction>,
}

impl ActionRegistry {
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
        }
    }

    /// Register a typed action. JSON Schema is auto-derived from `Input`.
    pub fn register<A: PlatformAction>(&mut self, action: A) {
        let schema = schema_for!(A::Input);
        let schema_value = serde_json::to_value(schema).expect("schema serialization");
        let mut schema_obj: serde_json::Map<String, serde_json::Value> = match schema_value {
            serde_json::Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };

        // Many LLM tool-calling APIs (Anthropic, OpenAI, Claude, etc.) reject
        // schemas that use `oneOf`/`anyOf` at the top level. `schemars` emits
        // these for internally-tagged enums (`#[serde(tag = "...")]`).
        //
        // We flatten them into a plain `{ type: "object", properties: { tag: { enum: [...] }, ...merged... } }`
        // schema. The tag field becomes a required string enum, and all variant
        // properties are merged into a single properties map. This loses
        // per-variant required-ness but is broadly compatible with tool APIs.
        schema_obj = flatten_oneof_schema(schema_obj);

        if !schema_obj.contains_key("type") {
            schema_obj.insert(
                "type".to_string(),
                serde_json::Value::String("object".to_string()),
            );
        }

        let tool = Tool::new(action.name(), action.description(), Arc::new(schema_obj));

        let name = action.name();
        let required_scope = action.required_scope();
        debug_assert!(
            !self.actions.contains_key(name),
            "Duplicate action name registered: {name}"
        );
        self.actions.insert(
            name,
            RegisteredAction {
                tool,
                required_scope,
                handler: Box::new(TypedHandler(action)),
            },
        );
    }

    /// Returns MCP `Tool` definitions for `tools/list`, sorted by name for
    /// stable ordering across runs.
    pub fn tools_list(&self) -> Vec<Tool> {
        let mut tools: Vec<Tool> = self.actions.values().map(|a| a.tool.clone()).collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }

    /// Returns only the tools the caller has scopes to invoke.
    pub fn tools_list_filtered(&self, scopes: &[String]) -> Vec<Tool> {
        let mut tools: Vec<Tool> = self
            .actions
            .values()
            .filter(|a| crate::scope::has_scope(scopes, &a.required_scope))
            .map(|a| a.tool.clone())
            .collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }

    /// Look up a tool definition by name (for `get_tool`).
    pub fn get_tool(&self, name: &str) -> Option<Tool> {
        self.actions.get(name).map(|a| a.tool.clone())
    }

    /// Dispatch a `tools/call` request.
    #[tracing::instrument(
        name = "mcp.registry.call_tool",
        skip(self, arguments, ctx),
        fields(gen_ai.tool.name = %name, project_id = %ctx.project_id)
    )]
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
        ctx: &ActionContext,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let action = match self.actions.get(name) {
            Some(a) => a,
            None => {
                return Err(rmcp::ErrorData::new(
                    rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                    format!("Unknown tool: {name}"),
                    None,
                ));
            }
        };

        if !crate::scope::has_scope(&ctx.scopes, &action.required_scope) {
            return Err(rmcp::ErrorData::new(
                rmcp::model::ErrorCode::INVALID_REQUEST,
                format!(
                    "Permission denied: requires scope '{}'",
                    action.required_scope
                ),
                None,
            ));
        }

        // Check credit allowance (hard cap for free tier)
        if let Some(denial_reason) = crate::metering::check_credit_allowance(ctx).await {
            return Ok(CallToolResult::error(vec![Content::text(denial_reason)]));
        }

        match action.handler.call(ctx, arguments.clone()).await {
            Ok(value) => {
                crate::metering::emit_credit_event(ctx, name, &arguments, uuid::Uuid::new_v4());
                let text =
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| format!("{value:?}"));
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e.to_string())])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{ActionContext, Caller};
    use crate::client::InternalClient;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    fn test_ctx() -> ActionContext {
        let pid = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        ActionContext {
            project_id: pid,
            caller: Caller::ApiKey {
                key_id: Uuid::nil(),
            },
            scopes: crate::scope::ALL_SCOPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            http: InternalClient::new(
                "http://test-website".into(),
                "http://test-flow".into(),
                "http://test-watch".into(),
                pid,
                "test-key".into(),
            ),
            db: None,
            clickhouse: None,
            encryptor: None,
            asset_storage: None,
            kb_embedder: None,
            meter_service: None,
            organization_id: None,
            entitlements: std::sync::Arc::new(reiver_core::entitlements::UnlimitedEntitlements),
            key_prefix: String::new(),
            key_label: String::new(),
        }
    }

    // ── Dummy action used by most tests ──────────────────────────────

    struct EchoAction;

    #[derive(Deserialize, JsonSchema)]
    struct EchoInput {
        pub value: String,
    }

    #[derive(Serialize)]
    struct EchoOutput {
        pub echo: String,
    }

    #[async_trait]
    impl PlatformAction for EchoAction {
        type Input = EchoInput;
        type Output = EchoOutput;
        fn name(&self) -> &'static str {
            "echo"
        }
        fn description(&self) -> &'static str {
            "Echoes the input value"
        }
        fn required_scope(&self) -> String {
            "project:read".into()
        }
        async fn execute(
            &self,
            _ctx: &ActionContext,
            input: EchoInput,
        ) -> anyhow::Result<EchoOutput> {
            Ok(EchoOutput { echo: input.value })
        }
    }

    struct FailAction;

    #[derive(Deserialize, JsonSchema)]
    struct FailInput {}

    #[derive(Serialize)]
    struct FailOutput {}

    #[async_trait]
    impl PlatformAction for FailAction {
        type Input = FailInput;
        type Output = FailOutput;
        fn name(&self) -> &'static str {
            "fail"
        }
        fn description(&self) -> &'static str {
            "Always fails"
        }
        fn required_scope(&self) -> String {
            "project:read".into()
        }
        async fn execute(
            &self,
            _ctx: &ActionContext,
            _input: FailInput,
        ) -> anyhow::Result<FailOutput> {
            anyhow::bail!("intentional failure");
        }
    }

    // ── Second action for sorting test ───────────────────────────────

    struct AlphaAction;

    #[derive(Deserialize, JsonSchema)]
    struct AlphaInput {}

    #[derive(Serialize)]
    struct AlphaOutput {}

    #[async_trait]
    impl PlatformAction for AlphaAction {
        type Input = AlphaInput;
        type Output = AlphaOutput;
        fn name(&self) -> &'static str {
            "alpha"
        }
        fn description(&self) -> &'static str {
            "First alphabetically"
        }
        fn required_scope(&self) -> String {
            "project:read".into()
        }
        async fn execute(
            &self,
            _ctx: &ActionContext,
            _input: AlphaInput,
        ) -> anyhow::Result<AlphaOutput> {
            Ok(AlphaOutput {})
        }
    }

    struct ZetaAction;

    #[derive(Deserialize, JsonSchema)]
    struct ZetaInput {}

    #[derive(Serialize)]
    struct ZetaOutput {}

    #[async_trait]
    impl PlatformAction for ZetaAction {
        type Input = ZetaInput;
        type Output = ZetaOutput;
        fn name(&self) -> &'static str {
            "zeta"
        }
        fn description(&self) -> &'static str {
            "Last alphabetically"
        }
        fn required_scope(&self) -> String {
            "project:read".into()
        }
        async fn execute(
            &self,
            _ctx: &ActionContext,
            _input: ZetaInput,
        ) -> anyhow::Result<ZetaOutput> {
            Ok(ZetaOutput {})
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[test]
    fn test_register_and_tools_list() {
        let mut reg = ActionRegistry::new();
        reg.register(EchoAction);

        let tools = reg.tools_list();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name.as_ref(), "echo");
        assert_eq!(
            tools[0].description.as_deref(),
            Some("Echoes the input value")
        );

        let schema_val = tools[0].schema_as_json_value();
        let props = schema_val.get("properties");
        assert!(props.is_some(), "Schema should have a 'properties' key");
        assert!(
            props.unwrap().get("value").is_some(),
            "Schema properties should include 'value'"
        );
    }

    #[test]
    fn test_get_tool_found() {
        let mut reg = ActionRegistry::new();
        reg.register(EchoAction);

        let tool = reg.get_tool("echo");
        assert!(tool.is_some());
        assert_eq!(tool.unwrap().name.as_ref(), "echo");
    }

    #[test]
    fn test_get_tool_not_found() {
        let reg = ActionRegistry::new();
        assert!(reg.get_tool("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_call_tool_success() {
        let mut reg = ActionRegistry::new();
        reg.register(EchoAction);
        let ctx = test_ctx();

        let input = serde_json::json!({ "value": "hello" });
        let result = reg.call_tool("echo", input, &ctx).await;

        assert!(result.is_ok());
        let call_result = result.unwrap();
        assert!(!call_result.is_error.unwrap_or(false));

        let text = &call_result.content[0];
        let text_str = format!("{text:?}");
        assert!(
            text_str.contains("hello"),
            "Output should contain 'hello': {text_str}"
        );
    }

    #[tokio::test]
    async fn test_call_tool_unknown() {
        let reg = ActionRegistry::new();
        let ctx = test_ctx();

        let result = reg
            .call_tool("nonexistent", serde_json::json!({}), &ctx)
            .await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn test_call_tool_invalid_input() {
        let mut reg = ActionRegistry::new();
        reg.register(EchoAction);
        let ctx = test_ctx();

        // EchoInput requires a "value" field; passing an integer should fail deserialization
        let input = serde_json::json!({ "wrong_field": 42 });
        let result = reg.call_tool("echo", input, &ctx).await;

        // Should return Ok with an error result (tool-level error, not JSON-RPC error)
        assert!(result.is_ok());
        let call_result = result.unwrap();
        assert!(call_result.is_error.unwrap_or(false));
    }

    #[tokio::test]
    async fn test_call_tool_action_error() {
        let mut reg = ActionRegistry::new();
        reg.register(FailAction);
        let ctx = test_ctx();

        let result = reg.call_tool("fail", serde_json::json!({}), &ctx).await;

        assert!(result.is_ok());
        let call_result = result.unwrap();
        assert!(call_result.is_error.unwrap_or(false));
        let text = format!("{:?}", call_result.content[0]);
        assert!(text.contains("intentional failure"));
    }

    #[test]
    fn test_tools_list_sorted() {
        let mut reg = ActionRegistry::new();
        // Register in reverse alphabetical order
        reg.register(ZetaAction);
        reg.register(EchoAction);
        reg.register(AlphaAction);

        let tools = reg.tools_list();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert_eq!(names, vec!["alpha", "echo", "zeta"]);
    }

    // ── Schema quality verification ──────────────────────────────────

    fn schema_has_description(schema: &serde_json::Value, field: &str) -> bool {
        schema["properties"][field]["description"]
            .as_str()
            .map_or(false, |d| !d.is_empty())
    }

    fn schema_required_contains(schema: &serde_json::Value, field: &str) -> bool {
        schema["required"]
            .as_array()
            .map_or(false, |arr| arr.iter().any(|v| v.as_str() == Some(field)))
    }

    #[test]
    fn schema_create_alert_rule_has_typed_fields() {
        use crate::actions::alerting::CreateAlertRuleInput;
        let schema = serde_json::to_value(schemars::schema_for!(CreateAlertRuleInput)).unwrap();
        let rule = &schema["definitions"]["CreateAlertRuleData"];
        assert!(
            schema_required_contains(rule, "name"),
            "name should be required"
        );
        assert!(
            schema_required_contains(rule, "query_config"),
            "query_config should be required"
        );
        assert!(
            schema_has_description(rule, "name"),
            "name should have description"
        );
        assert!(
            schema_has_description(rule, "threshold"),
            "threshold should have description"
        );
    }

    #[test]
    fn schema_query_widget_has_typed_fields() {
        use crate::actions::dashboards::QueryWidgetInput;
        let schema = serde_json::to_value(schemars::schema_for!(QueryWidgetInput)).unwrap();
        assert!(
            schema_required_contains(&schema, "query"),
            "query should be required"
        );
        assert!(
            !schema_required_contains(&schema, "time_range"),
            "time_range should NOT be required (has default)"
        );
        let pq = &schema["definitions"]["PromQLQueryConfig"];
        assert!(
            schema_required_contains(pq, "promql"),
            "promql should be required"
        );
    }

    fn find_enum_values(schema: &serde_json::Value, type_name: &str) -> Vec<String> {
        let def = &schema["definitions"][type_name];
        if let Some(vals) = def["enum"].as_array() {
            return vals
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(one_of) = def["oneOf"].as_array() {
            return one_of
                .iter()
                .filter_map(|v| v["enum"].as_array())
                .flatten()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        vec![]
    }

    #[test]
    fn schema_llm_provider_emits_enum_values() {
        use crate::actions::flow::integrations::ConfigureIntegrationInput;
        let schema =
            serde_json::to_value(schemars::schema_for!(ConfigureIntegrationInput)).unwrap();
        let vals = find_enum_values(&schema, "LlmProvider");
        for expected in &["openai", "anthropic", "google", "theta", "bedrock"] {
            assert!(
                vals.contains(&expected.to_string()),
                "LlmProvider should contain '{}', got: {:?}",
                expected,
                vals
            );
        }
    }

    #[test]
    fn schema_message_role_emits_enum_values() {
        use crate::actions::flow::playground::RunPlaygroundInput;
        let schema = serde_json::to_value(schemars::schema_for!(RunPlaygroundInput)).unwrap();
        let vals = find_enum_values(&schema, "MessageRole");
        for expected in &["system", "user", "assistant"] {
            assert!(
                vals.contains(&expected.to_string()),
                "MessageRole should contain '{}', got: {:?}",
                expected,
                vals
            );
        }
    }

    #[test]
    fn schema_temperature_has_range() {
        use crate::actions::flow::playground::RunPlaygroundInput;
        let schema = serde_json::to_value(schemars::schema_for!(RunPlaygroundInput)).unwrap();
        let temp = &schema["properties"]["temperature"];
        let max = temp["maximum"]
            .as_f64()
            .or_else(|| temp["allOf"][0]["maximum"].as_f64());
        assert!(
            max.is_some(),
            "temperature should have a maximum constraint"
        );
        assert!(max.unwrap() <= 1.0, "temperature max should be <= 1.0");
    }

    #[test]
    fn schema_playground_model_uses_live_catalog_or_project_routing() {
        use crate::actions::flow::playground::RunPlaygroundInput;
        let schema = serde_json::to_value(schemars::schema_for!(RunPlaygroundInput)).unwrap();
        let description = schema["properties"]["model"]["description"]
            .as_str()
            .expect("model should have a description");
        assert!(description.contains("model_catalog"));
        assert!(description.contains("project"));
        assert!(!schema_required_contains(&schema, "model"));
    }

    #[test]
    fn schema_gateway_settings_has_guardrails() {
        use crate::actions::flow::settings::UpdateGatewaySettingsInputWrapper;
        let schema =
            serde_json::to_value(schemars::schema_for!(UpdateGatewaySettingsInputWrapper)).unwrap();
        let gs = &schema["definitions"]["GatewaySettingsInput"];
        assert!(
            schema_has_description(gs, "introspection_enabled"),
            "should have description"
        );
        assert!(
            schema_has_description(gs, "guardrails"),
            "guardrails should have description"
        );
        assert!(
            schema_has_description(gs, "default_fallback_models"),
            "default_fallback_models should have description"
        );
        assert!(
            schema_has_description(gs, "provider_preferences"),
            "provider_preferences should have description"
        );

        let prefs = &schema["definitions"]["ProviderPreferencesInput"];
        for field in &["order", "only", "ignore", "allow_fallbacks", "sort"] {
            assert!(
                schema_has_description(prefs, field),
                "ProviderPreferencesInput.{} should have a description",
                field
            );
        }
    }

    // ── Enum serialization round-trip tests ─────────────────────────

    #[test]
    fn enum_llm_provider_serialization() {
        use crate::actions::types::LlmProvider;
        let cases = vec![
            (LlmProvider::Openai, "openai"),
            (LlmProvider::Anthropic, "anthropic"),
            (LlmProvider::Google, "google"),
            (LlmProvider::Theta, "theta"),
            (LlmProvider::Bedrock, "bedrock"),
        ];
        for (variant, expected) in cases {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(
                val.as_str().unwrap(),
                expected,
                "LlmProvider::{:?} should serialize to '{}'",
                variant,
                expected
            );
            let back: LlmProvider = serde_json::from_value(val).unwrap();
            assert_eq!(
                serde_json::to_value(&back).unwrap().as_str().unwrap(),
                expected
            );
        }
    }

    #[test]
    fn enum_log_level_serialization() {
        use crate::actions::types::LogLevel;
        for (variant, expected) in [
            (LogLevel::Error, "error"),
            (LogLevel::Warn, "warn"),
            (LogLevel::Info, "info"),
            (LogLevel::Debug, "debug"),
            (LogLevel::Trace, "trace"),
        ] {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(val.as_str().unwrap(), expected);
        }
    }

    #[test]
    fn enum_auth_provider_kind_serialization() {
        use crate::actions::types::AuthProviderKind;
        for (variant, expected) in [
            (AuthProviderKind::Okta, "okta"),
            (AuthProviderKind::Auth0, "auth0"),
            (AuthProviderKind::EntraId, "entra_id"),
            (AuthProviderKind::OneLogin, "onelogin"),
            (AuthProviderKind::PingIdentity, "ping_identity"),
            (AuthProviderKind::Keycloak, "keycloak"),
        ] {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(
                val.as_str().unwrap(),
                expected,
                "{:?} should serialize to '{}'",
                variant,
                expected
            );
        }
    }

    #[test]
    fn enum_aws_auth_method_serialization() {
        use crate::actions::types::AwsAuthMethod;
        assert_eq!(serde_json::to_value(&AwsAuthMethod::Role).unwrap(), "role");
        assert_eq!(
            serde_json::to_value(&AwsAuthMethod::AccessKey).unwrap(),
            "access_key"
        );
    }

    #[test]
    fn enum_notification_channel_type_serialization() {
        use crate::actions::types::NotificationChannelType;
        for (variant, expected) in [
            (NotificationChannelType::Slack, "slack"),
            (NotificationChannelType::Teams, "teams"),
            (NotificationChannelType::Discord, "discord"),
            (NotificationChannelType::Pagerduty, "pagerduty"),
            (NotificationChannelType::Webhook, "webhook"),
        ] {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(val.as_str().unwrap(), expected);
        }
    }

    #[test]
    fn enum_aws_service_type_serialization() {
        use crate::actions::types::AwsServiceType;
        for (variant, expected) in [
            (AwsServiceType::Ec2, "ec2"),
            (AwsServiceType::Rds, "rds"),
            (AwsServiceType::Lambda, "lambda"),
            (AwsServiceType::S3, "s3"),
            (AwsServiceType::Ecs, "ecs"),
            (AwsServiceType::Eks, "eks"),
            (AwsServiceType::DynamoDb, "dynamodb"),
            (AwsServiceType::Sqs, "sqs"),
            (AwsServiceType::Sns, "sns"),
        ] {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(
                val.as_str().unwrap(),
                expected,
                "{:?} should serialize to '{}'",
                variant,
                expected
            );
        }
    }

    // ── Typed struct serialization tests ────────────────────────────

    #[test]
    fn create_alert_rule_data_serializes_correctly() {
        use crate::actions::types::{AlertQueryConfigInput, CreateAlertRuleData};
        let rule = CreateAlertRuleData {
            name: "High latency".into(),
            description: Some("P95 > 500ms".into()),
            query_config: AlertQueryConfigInput {
                query_type: "metrics".into(),
                metric_name: Some("http.server.duration".into()),
                filters: [("service.name".into(), "api".into())]
                    .into_iter()
                    .collect(),
                group_by: vec![],
                time_aggregation: "p95".into(),
                space_aggregation: "avg".into(),
                patterns: None,
                log_source: None,
                promql: None,
            },
            threshold: 500.0,
            threshold_type: crate::actions::types::ThresholdType::Above,
            notification_channels: vec!["ch-1".into()],
            alert_on_absent: false,
            absent_for_seconds: 300,
            eval_window_seconds: 300,
            eval_interval_seconds: 60,
            labels: Default::default(),
            annotations: Default::default(),
            enabled: true,
        };

        let val = serde_json::to_value(&rule).unwrap();
        assert_eq!(val["name"], "High latency");
        assert_eq!(val["threshold"], 500.0);
        assert_eq!(val["threshold_type"], "above");
        assert_eq!(val["query_config"]["metric_name"], "http.server.duration");
        assert_eq!(val["query_config"]["filters"]["service.name"], "api");
        assert_eq!(val["notification_channels"][0], "ch-1");
    }

    #[test]
    fn update_alert_rule_data_omits_none_as_null() {
        use crate::actions::types::UpdateAlertRuleData;
        let update = UpdateAlertRuleData {
            name: Some("Renamed".into()),
            description: None,
            query_config: None,
            threshold: Some(100.0),
            threshold_type: None,
            notification_channels: None,
            alert_on_absent: None,
            absent_for_seconds: None,
            eval_window_seconds: None,
            eval_interval_seconds: None,
            labels: None,
            annotations: None,
            enabled: None,
        };

        let val = serde_json::to_value(&update).unwrap();
        assert_eq!(val["name"], "Renamed");
        assert_eq!(val["threshold"], 100.0);
        assert!(val["description"].is_null(), "unset fields should be null");
        assert!(val["query_config"].is_null());
        assert!(val["enabled"].is_null());
    }

    #[test]
    fn promql_query_config_serializes_field_names_correctly() {
        use crate::actions::types::{PromQLQueryConfig, PromQLSubQuery};
        let qc = PromQLQueryConfig {
            promql: "rate(http_requests_total[5m])".into(),
            legend_format: Some("{{method}} {{status}}".into()),
            queries: Some(vec![PromQLSubQuery {
                promql: "rate(http_errors_total[5m])".into(),
                legend_format: Some("errors".into()),
            }]),
            instant: Some(false),
        };

        let val = serde_json::to_value(&qc).unwrap();
        assert_eq!(val["promql"], "rate(http_requests_total[5m])");
        assert_eq!(val["legend_format"], "{{method}} {{status}}");
        assert_eq!(val["queries"][0]["promql"], "rate(http_errors_total[5m])");
        assert_eq!(val["queries"][0]["legend_format"], "errors");
        assert_eq!(val["instant"], false);
    }

    // ── Additional schema completeness tests ────────────────────────

    #[test]
    fn schema_all_enum_types_emit_expected_values() {
        use crate::actions::watch::logs::SearchLogsInput;
        let schema = serde_json::to_value(schemars::schema_for!(SearchLogsInput)).unwrap();
        let vals = find_enum_values(&schema, "LogLevel");
        for expected in &["error", "warn", "info", "debug", "trace"] {
            assert!(
                vals.contains(&expected.to_string()),
                "LogLevel should contain '{}', got: {:?}",
                expected,
                vals
            );
        }
    }

    #[test]
    fn schema_notification_channel_type_emits_enum_values() {
        use crate::actions::watch::notification_channels::ConfigureNotificationChannelInput;
        let schema =
            serde_json::to_value(schemars::schema_for!(ConfigureNotificationChannelInput)).unwrap();
        let vals = find_enum_values(&schema, "NotificationChannelType");
        for expected in &["slack", "teams", "discord", "pagerduty", "webhook"] {
            assert!(
                vals.contains(&expected.to_string()),
                "NotificationChannelType should contain '{}', got: {:?}",
                expected,
                vals
            );
        }
    }

    #[test]
    fn schema_promql_query_config_has_promql_field() {
        use crate::actions::dashboards::QueryWidgetInput;
        let schema = serde_json::to_value(schemars::schema_for!(QueryWidgetInput)).unwrap();
        let pq = &schema["definitions"]["PromQLQueryConfig"];
        assert!(
            schema_required_contains(pq, "promql"),
            "promql should be required in PromQLQueryConfig"
        );
        assert!(
            schema_has_description(pq, "promql"),
            "promql should have a description"
        );
    }

    #[test]
    fn schema_guardrail_config_has_all_fields() {
        use crate::actions::flow::settings::UpdateGatewaySettingsInputWrapper;
        let schema =
            serde_json::to_value(schemars::schema_for!(UpdateGatewaySettingsInputWrapper)).unwrap();
        let gc = &schema["definitions"]["GuardrailConfigInput"];
        for field in &[
            "trust_mode",
            "blocked_input_topics",
            "max_prompt_tokens",
            "pii_block_on_detect",
            "prompt_injection_detection",
            "spotlighting_enabled",
            "mask_output_pii",
            "blocked_output_topics",
            "min_quality_score",
            "blocked_tools",
            "block_exfiltration_urls",
        ] {
            assert!(
                schema_has_description(gc, field),
                "GuardrailConfigInput.{} should have a description",
                field
            );
        }
    }

    #[test]
    fn schema_thinking_budget_has_range() {
        use crate::actions::flow::settings::UpdateGatewaySettingsInputWrapper;
        let schema =
            serde_json::to_value(schemars::schema_for!(UpdateGatewaySettingsInputWrapper)).unwrap();
        let gs = &schema["definitions"]["GatewaySettingsInput"];
        let tb = &gs["properties"]["thinking_budget_tokens"];
        let max = tb["maximum"].as_f64().or_else(|| {
            tb["allOf"]
                .as_array()
                .and_then(|a| a.iter().find_map(|v| v["maximum"].as_f64()))
        });
        assert!(
            max.is_some(),
            "thinking_budget_tokens should have a max constraint"
        );
        assert_eq!(max.unwrap(), 200000.0);
    }

    // ── Full registration: uniqueness + minimum count ───────────────

    #[test]
    fn register_all_produces_unique_tool_names() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tools = reg.tools_list();

        let mut seen = std::collections::HashSet::new();
        for tool in &tools {
            assert!(
                seen.insert(tool.name.as_ref()),
                "Duplicate tool name: {}",
                tool.name
            );
        }
    }

    #[test]
    fn register_all_has_exactly_five_facade_tools() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tools = reg.tools_list();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert_eq!(
            tools.len(),
            5,
            "Expected exactly 5 facade tools, got {}: {:?}",
            tools.len(),
            names
        );
        for expected in &["search", "get", "list", "analyze", "execute"] {
            assert!(
                names.contains(expected),
                "Missing facade tool '{}', registered: {:?}",
                expected,
                names,
            );
        }
    }

    #[test]
    fn all_tools_have_descriptions() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        for tool in reg.tools_list() {
            let desc = tool.description.as_deref().unwrap_or("");
            assert!(
                !desc.is_empty(),
                "Tool '{}' has an empty description",
                tool.name,
            );
        }
    }

    #[test]
    fn facade_tools_all_accessible_with_project_read() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let scopes = vec!["project:read".to_string()];
        let visible = reg.tools_list_filtered(&scopes);
        assert_eq!(
            visible.len(),
            5,
            "All 5 facade tools should be visible with project:read scope, got {}",
            visible.len(),
        );
    }

    // ── Exception status enum now includes 'ignored' ────────────────

    #[test]
    fn exception_status_includes_ignored() {
        use crate::actions::types::ExceptionStatus;
        let val = serde_json::to_value(&ExceptionStatus::Ignored).unwrap();
        assert_eq!(val, "ignored");
        let back: ExceptionStatus = serde_json::from_str("\"ignored\"").unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), "ignored");
    }

    // ── Health check type enum ──────────────────────────────────────

    #[test]
    fn health_check_type_serialization() {
        use crate::actions::watch::health_checks::HealthCheckType;
        for (variant, expected) in [
            (HealthCheckType::Http, "http"),
            (HealthCheckType::Tcp, "tcp"),
            (HealthCheckType::Udp, "udp"),
            (HealthCheckType::Ssl, "ssl"),
        ] {
            let val = serde_json::to_value(&variant).unwrap();
            assert_eq!(val.as_str().unwrap(), expected);
        }
    }

    // ── Score type enum ─────────────────────────────────────────────

    #[test]
    fn score_type_serialization() {
        use crate::actions::flow::scores::ScoreType;
        assert_eq!(serde_json::to_value(&ScoreType::Number).unwrap(), "number");
        assert_eq!(
            serde_json::to_value(&ScoreType::Boolean).unwrap(),
            "boolean"
        );
    }

    // ── Facade schema tests ─────────────────────────────────────────

    #[test]
    fn all_tool_schemas_have_type_object() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        for tool in reg.tools_list() {
            let schema = tool.schema_as_json_value();
            let obj = schema.as_object().expect("schema must be an object");
            assert_eq!(
                obj.get("type").and_then(|v| v.as_str()),
                Some("object"),
                "Tool '{}' schema must have \"type\": \"object\" at root for LLM provider compatibility",
                tool.name
            );
        }
    }

    #[test]
    fn search_tool_schema_has_source_discriminator() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tool = reg.get_tool("search").expect("search tool should exist");
        let schema = tool.schema_as_json_value();
        let schema_str = serde_json::to_string(&schema).unwrap();
        assert!(
            schema_str.contains("llm_requests"),
            "search schema should list llm_requests source"
        );
        assert!(
            schema_str.contains("logs"),
            "search schema should list logs source"
        );
        assert!(
            schema_str.contains("web"),
            "search schema should list web source"
        );
    }

    #[test]
    fn get_tool_schema_has_resource_discriminator() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tool = reg.get_tool("get").expect("get tool should exist");
        let schema = tool.schema_as_json_value();
        let schema_str = serde_json::to_string(&schema).unwrap();
        for resource in &[
            "trace",
            "session",
            "log",
            "exception",
            "incident",
            "dashboard",
            "health_check",
            "prompt_config",
            "project",
            "gateway_settings",
        ] {
            assert!(
                schema_str.contains(resource),
                "get schema should mention resource '{}', schema: {}",
                resource,
                &schema_str[..200.min(schema_str.len())]
            );
        }
    }

    #[test]
    fn list_tool_schema_has_resource_discriminator() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tool = reg.get_tool("list").expect("list tool should exist");
        let schema = tool.schema_as_json_value();
        let schema_str = serde_json::to_string(&schema).unwrap();
        for resource in &[
            "traces",
            "services",
            "sessions",
            "exceptions",
            "incidents",
            "dashboards",
            "alert_rules",
            "health_checks",
            "prompt_configs",
            "projects",
            "model_catalog",
        ] {
            assert!(
                schema_str.contains(resource),
                "list schema should mention resource '{}'",
                resource
            );
        }
    }

    #[test]
    fn analyze_tool_schema_has_analysis_discriminator() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tool = reg.get_tool("analyze").expect("analyze tool should exist");
        let schema = tool.schema_as_json_value();
        let schema_str = serde_json::to_string(&schema).unwrap();
        for analysis in &[
            "llm_overview",
            "widget_query",
            "dashboard_snapshot",
            "playground",
            "root_cause",
            "usage",
        ] {
            assert!(
                schema_str.contains(analysis),
                "analyze schema should mention analysis type '{}'",
                analysis
            );
        }
    }

    #[test]
    fn execute_tool_schema_has_resource_and_action_fields() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let tool = reg.get_tool("execute").expect("execute tool should exist");
        let schema = tool.schema_as_json_value();
        assert!(
            schema["properties"]["resource"].is_object(),
            "execute should have resource property"
        );
        assert!(
            schema["properties"]["action"].is_object(),
            "execute should have action property"
        );
        assert!(
            schema["properties"]["params"].is_object(),
            "execute should have params property"
        );
    }

    #[test]
    fn facade_search_input_deserializes_llm_requests() {
        use crate::actions::facade::search::SearchInput;
        let json = serde_json::json!({
            "source": "llm_requests",
            "query": "test error"
        });
        let input: SearchInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, SearchInput::LlmRequests(_)));
    }

    #[test]
    fn facade_search_input_deserializes_logs() {
        use crate::actions::facade::search::SearchInput;
        let json = serde_json::json!({
            "source": "logs",
            "query": "timeout"
        });
        let input: SearchInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, SearchInput::Logs(_)));
    }

    #[test]
    fn facade_get_input_deserializes_trace() {
        use crate::actions::facade::get::GetInput;
        let json = serde_json::json!({
            "resource": "trace",
            "trace_id": "abc123"
        });
        let input: GetInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, GetInput::Trace(_)));
    }

    #[test]
    fn facade_execute_input_deserializes() {
        use crate::actions::facade::execute_action::ExecuteInput;
        let json = serde_json::json!({
            "resource": "prompt",
            "action": "deploy",
            "params": { "config_id": "abc", "version_id": "def" }
        });
        let input: ExecuteInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.resource, "prompt");
        assert_eq!(input.action, "deploy");
    }

    #[test]
    fn facade_list_input_deserializes_traces() {
        use crate::actions::facade::list::ListInput;
        let json = serde_json::json!({
            "resource": "traces",
            "service": "api-gateway",
            "limit": 50
        });
        let input: ListInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, ListInput::Traces(_)));
    }

    #[test]
    fn facade_list_input_deserializes_empty_struct_variant() {
        use crate::actions::facade::list::ListInput;
        let json = serde_json::json!({ "resource": "alert_rules" });
        let input: ListInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, ListInput::AlertRules(_)));
    }

    #[test]
    fn facade_list_input_deserializes_model_catalog() {
        use crate::actions::facade::list::ListInput;
        let json = serde_json::json!({ "resource": "model_catalog" });
        let input: ListInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, ListInput::ModelCatalog(_)));
    }

    #[test]
    fn facade_analyze_input_deserializes_llm_overview() {
        use crate::actions::facade::analyze::AnalyzeInput;
        let json = serde_json::json!({
            "analysis": "llm_overview",
            "start_date": "2026-04-01",
            "end_date": "2026-04-07"
        });
        let input: AnalyzeInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, AnalyzeInput::LlmOverview(_)));
    }

    #[test]
    fn facade_get_input_deserializes_gateway_settings_empty() {
        use crate::actions::facade::get::GetInput;
        let json = serde_json::json!({ "resource": "gateway_settings" });
        let input: GetInput = serde_json::from_value(json).unwrap();
        assert!(matches!(input, GetInput::GatewaySettings(_)));
    }

    #[test]
    fn facade_search_rejects_unknown_source() {
        use crate::actions::facade::search::SearchInput;
        let json = serde_json::json!({
            "source": "nosuchsource",
            "query": "test"
        });
        assert!(serde_json::from_value::<SearchInput>(json).is_err());
    }

    #[test]
    fn facade_get_rejects_unknown_resource() {
        use crate::actions::facade::get::GetInput;
        let json = serde_json::json!({
            "resource": "warehouse_source",
            "id": "abc"
        });
        assert!(serde_json::from_value::<GetInput>(json).is_err());
    }

    #[tokio::test]
    async fn facade_execute_unknown_resource_returns_error() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let ctx = test_ctx();

        let input = serde_json::json!({
            "resource": "nonexistent",
            "action": "create",
            "params": {}
        });
        let result = reg.call_tool("execute", input, &ctx).await.unwrap();
        assert!(result.is_error.unwrap_or(false));
        let text = format!("{:?}", result.content[0]);
        assert!(
            text.contains("Unknown resource/action"),
            "Error should mention unknown resource, got: {text}"
        );
    }

    #[tokio::test]
    async fn facade_execute_unknown_action_for_known_resource_returns_error() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);
        let ctx = test_ctx();

        let input = serde_json::json!({
            "resource": "project",
            "action": "destroy",
            "params": {}
        });
        let result = reg.call_tool("execute", input, &ctx).await.unwrap();
        assert!(result.is_error.unwrap_or(false));
        let text = format!("{:?}", result.content[0]);
        assert!(
            text.contains("Unknown resource/action"),
            "Error should mention unknown resource/action, got: {text}"
        );
    }

    #[test]
    fn facade_execute_github_merges_action_into_params() {
        use crate::actions::facade::execute_action::ExecuteInput;
        let json = serde_json::json!({
            "resource": "github",
            "action": "list_commits",
            "params": { "branch": "main", "limit": 5 }
        });
        let input: ExecuteInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.resource, "github");
        assert_eq!(input.action, "list_commits");
        assert_eq!(input.params["branch"], "main");
    }

    #[tokio::test]
    async fn facade_scope_enforcement_blocks_unauthorized_access() {
        let mut reg = ActionRegistry::new();
        crate::actions::register_all(&mut reg);

        let pid = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let restricted_ctx = ActionContext {
            project_id: pid,
            caller: Caller::ApiKey {
                key_id: Uuid::nil(),
            },
            scopes: vec!["project:read".to_string()],
            http: crate::client::InternalClient::new(
                "http://test-website".into(),
                "http://test-flow".into(),
                "http://test-watch".into(),
                pid,
                "test-key".into(),
            ),
            db: None,
            clickhouse: None,
            encryptor: None,
            asset_storage: None,
            kb_embedder: None,
            meter_service: None,
            organization_id: None,
            entitlements: std::sync::Arc::new(reiver_core::entitlements::UnlimitedEntitlements),
            key_prefix: String::new(),
            key_label: String::new(),
        };

        let input = serde_json::json!({
            "resource": "alert_rule",
            "action": "create",
            "params": { "name": "test" }
        });
        let result = reg
            .call_tool("execute", input, &restricted_ctx)
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false));
        let text = format!("{:?}", result.content[0]);
        assert!(
            text.contains("Permission denied"),
            "Should reject with scope error, got: {text}"
        );
    }
}
