pub mod instant_manipulate;
pub mod range_manipulate;
pub mod series_divide;
pub mod series_normalize;
pub mod topk_bottomk;

use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

pub use range_manipulate::Millisecond;

/// Custom physical planner that converts our PromQL logical nodes into physical execution plans.
pub struct PromExtensionPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(sd) = node.as_any().downcast_ref::<series_divide::SeriesDivide>() {
            let input = physical_inputs[0].clone();
            return Ok(Some(Arc::new(series_divide::SeriesDivideExec::new(
                sd.tag_columns.clone(),
                input,
            ))));
        }

        if let Some(rm) = node
            .as_any()
            .downcast_ref::<range_manipulate::RangeManipulate>()
        {
            let input = physical_inputs[0].clone();
            let output_schema = rm.output_schema.inner().clone();
            return Ok(Some(Arc::new(range_manipulate::RangeManipulateExec::new(
                rm.start,
                rm.end,
                rm.interval,
                rm.range,
                rm.time_index.clone(),
                rm.field_columns.clone(),
                input,
                output_schema,
            ))));
        }

        if let Some(im) = node
            .as_any()
            .downcast_ref::<instant_manipulate::InstantManipulate>()
        {
            let input = physical_inputs[0].clone();
            return Ok(Some(Arc::new(
                instant_manipulate::InstantManipulateExec::new(
                    im.start,
                    im.end,
                    im.lookback_delta,
                    im.interval,
                    im.time_index_column.clone(),
                    im.field_column.clone(),
                    input,
                ),
            )));
        }

        if let Some(sn) = node
            .as_any()
            .downcast_ref::<series_normalize::SeriesNormalize>()
        {
            let input = physical_inputs[0].clone();
            return Ok(Some(Arc::new(series_normalize::SeriesNormalizeExec::new(
                sn.offset,
                sn.time_index_column.clone(),
                sn.field_column.clone(),
                input,
            ))));
        }

        if let Some(tk) = node
            .as_any()
            .downcast_ref::<topk_bottomk::TopkBottomk>()
        {
            let input = physical_inputs[0].clone();
            return Ok(Some(Arc::new(topk_bottomk::TopkBottomkExec::new(
                tk.k,
                tk.is_topk,
                tk.partition_columns.clone(),
                input,
            ))));
        }

        Ok(None)
    }
}
