use std::sync::Arc;

use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    Annotated, CallToolRequestParams, CallToolResult, Implementation, ListResourcesResult,
    ListToolsResult, PaginatedRequestParams, RawResource, ReadResourceRequestParams,
    ReadResourceResult, ResourceContents, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::service::RoleServer;
use tracing::Instrument;

use crate::action::ActionContext;
use crate::docs;
use crate::registry::ActionRegistry;

/// MCP server backed by the shared [`ActionRegistry`].
pub struct McpServer {
    registry: Arc<ActionRegistry>,
    context: ActionContext,
}

impl McpServer {
    pub fn new(registry: Arc<ActionRegistry>, context: ActionContext) -> Self {
        Self { registry, context }
    }
}

impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info.server_info = Implementation::from_build_env();
        info.instructions = Some(
            "Reiver platform MCP server. \
             Platform operations (querying data, managing dashboards, alerts, prompts, billing) \
             are performed through these tools. REST API endpoints and SDKs described in the \
             documentation resources are for application integration and require application \
             API keys — they do not accept agent tokens."
                .into(),
        );
        info
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        let span = tracing::info_span!(
            "mcp.server.list_tools",
            project_id = %self.context.project_id,
        );
        async move {
            let mut result = ListToolsResult::default();
            result.tools = self.registry.tools_list_filtered(&self.context.scopes);
            Ok(result)
        }
        .instrument(span)
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.registry.get_tool(name)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
        let tool_name = request.name.to_string();
        let span = tracing::info_span!(
            "mcp.server.call_tool",
            tool_name = %tool_name,
            project_id = %self.context.project_id,
            key_prefix = %self.context.key_prefix,
            key_label = %self.context.key_label,
        );
        async move {
            let arguments = request
                .arguments
                .map(|m| serde_json::Value::Object(m.into_iter().collect()))
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

            self.registry
                .call_tool(&request.name, arguments, &self.context)
                .await
        }
        .instrument(span)
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, rmcp::ErrorData>> + Send + '_
    {
        let resources = docs::ALL_DOCS
            .iter()
            .map(|doc| Annotated {
                raw: RawResource {
                    uri: doc.uri.to_string(),
                    name: doc.name.to_string(),
                    title: None,
                    description: Some(doc.description.to_string()),
                    mime_type: Some("text/markdown".to_string()),
                    size: None,
                    icons: None,
                    meta: None,
                },
                annotations: None,
            })
            .collect();
        std::future::ready(Ok(ListResourcesResult {
            resources,
            ..Default::default()
        }))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResult, rmcp::ErrorData>> + Send + '_
    {
        let result = match docs::find_doc(&request.uri) {
            Some(doc) => Ok(ReadResourceResult::new(vec![ResourceContents::text(
                doc.content,
                &request.uri,
            )
            .with_mime_type("text/markdown")])),
            None => Err(rmcp::ErrorData::resource_not_found(
                format!("Resource not found: {}", request.uri),
                None,
            )),
        };
        std::future::ready(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{ActionContext, Caller, PlatformAction};
    use crate::client::InternalClient;
    use async_trait::async_trait;
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
                "http://test".into(),
                "http://test".into(),
                "http://test".into(),
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

    struct PingAction;

    #[derive(Deserialize, JsonSchema)]
    struct PingInput {}

    #[derive(Serialize)]
    struct PingOutput {
        pong: bool,
    }

    #[async_trait]
    impl PlatformAction for PingAction {
        type Input = PingInput;
        type Output = PingOutput;
        fn name(&self) -> &'static str {
            "ping"
        }
        fn description(&self) -> &'static str {
            "Returns pong"
        }
        fn required_scope(&self) -> String {
            "project:read".into()
        }
        async fn execute(
            &self,
            _ctx: &ActionContext,
            _input: PingInput,
        ) -> anyhow::Result<PingOutput> {
            Ok(PingOutput { pong: true })
        }
    }

    #[test]
    fn test_server_get_info() {
        let registry = Arc::new(ActionRegistry::new());
        let server = McpServer::new(registry, test_ctx());

        let info = server.get_info();
        assert!(info.instructions.is_some());
        assert!(info.instructions.unwrap().contains("Reiver"));
    }

    #[test]
    fn test_server_get_tool_found() {
        let mut registry = ActionRegistry::new();
        registry.register(PingAction);
        let registry = Arc::new(registry);
        let server = McpServer::new(registry, test_ctx());

        let tool = server.get_tool("ping");
        assert!(tool.is_some());
        assert_eq!(tool.unwrap().name.as_ref(), "ping");
    }

    #[test]
    fn test_server_get_tool_not_found() {
        let registry = Arc::new(ActionRegistry::new());
        let server = McpServer::new(registry, test_ctx());

        assert!(server.get_tool("nonexistent").is_none());
    }

    #[test]
    fn test_action_context_construction() {
        let pid = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let key_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();

        let ctx = ActionContext {
            project_id: pid,
            caller: Caller::ApiKey { key_id },
            scopes: crate::scope::ALL_SCOPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            http: InternalClient::new(
                "http://w".into(),
                "http://f".into(),
                "http://v".into(),
                pid,
                "k".into(),
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

        assert_eq!(ctx.project_id, pid);
        match ctx.caller {
            Caller::ApiKey { key_id: k } => assert_eq!(k, key_id),
            _ => panic!("Expected ApiKey caller"),
        }
    }

    #[test]
    fn test_caller_user_variant() {
        let uid = Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap();
        let caller = Caller::User {
            user_id: uid,
            jwt: String::new(),
        };

        match caller {
            Caller::User { user_id, .. } => assert_eq!(user_id, uid),
            _ => panic!("Expected User caller"),
        }
    }
}
