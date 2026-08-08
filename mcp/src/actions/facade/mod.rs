pub mod analyze;
pub mod execute_action;
pub mod get;
pub mod herd;
pub mod list;
pub mod search;

use crate::action::ActionContext;
use crate::registry::ActionRegistry;

pub(crate) fn require_scope(ctx: &ActionContext, scope: &str) -> anyhow::Result<()> {
    if !crate::scope::has_scope(&ctx.scopes, scope) {
        anyhow::bail!("Permission denied: requires scope '{scope}'")
    }
    Ok(())
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(search::SearchTool);
    registry.register(get::GetTool);
    registry.register(list::ListTool);
    registry.register(analyze::AnalyzeTool);
    registry.register(execute_action::ExecuteTool);
}
