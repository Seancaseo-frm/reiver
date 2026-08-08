// Adapted from GreptimeDB — Apache License 2.0
// Handles the `offset` modifier by adjusting timestamps, and filters NaN values.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch, TimestampMillisecondArray};
use arrow_schema::SchemaRef;
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_expr::col;
use futures_util::{ready, Stream, StreamExt};

use super::range_manipulate::Millisecond;

// ── Logical Node ──

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesNormalize {
    pub offset: Millisecond,
    pub time_index_column: String,
    pub field_column: Option<String>,
    pub input: LogicalPlan,
}

impl SeriesNormalize {
    pub fn new(
        offset: Millisecond,
        time_index_column: String,
        field_column: Option<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            offset,
            time_index_column,
            field_column,
            input,
        }
    }
}

impl UserDefinedLogicalNodeCore for SeriesNormalize {
    fn name(&self) -> &str {
        "SeriesNormalize"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![col(self.time_index_column.as_str())];
        if let Some(field) = &self.field_column {
            exprs.push(col(field.as_str()));
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromSeriesNormalize: offset=[{}], time_index=[{}]",
            self.offset, self.time_index_column
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        Ok(Self::new(
            self.offset,
            self.time_index_column.clone(),
            self.field_column.clone(),
            inputs.into_iter().next().unwrap(),
        ))
    }
}

// ── Physical Exec ──

#[derive(Debug)]
pub struct SeriesNormalizeExec {
    pub offset: Millisecond,
    pub time_index_column: String,
    pub field_column: Option<String>,
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl SeriesNormalizeExec {
    pub fn new(
        offset: Millisecond,
        time_index_column: String,
        field_column: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            offset,
            time_index_column,
            field_column,
            input,
            properties,
        }
    }
}

impl ExecutionPlan for SeriesNormalizeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.offset,
            self.time_index_column.clone(),
            self.field_column.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let ts_col_index = schema
            .column_with_name(&self.time_index_column)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "time index column '{}' not found",
                    self.time_index_column
                ))
            })?;
        let field_col_index = self
            .field_column
            .as_ref()
            .and_then(|name| schema.column_with_name(name).map(|(idx, _)| idx));

        Ok(Box::pin(SeriesNormalizeStream {
            offset: self.offset,
            ts_col_index,
            field_col_index,
            schema,
            input,
        }))
    }

    fn name(&self) -> &str {
        "SeriesNormalizeExec"
    }
}

impl DisplayAs for SeriesNormalizeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PromSeriesNormalizeExec: offset=[{}]", self.offset)
    }
}

// ── Stream ──

pub struct SeriesNormalizeStream {
    offset: Millisecond,
    ts_col_index: usize,
    field_col_index: Option<usize>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl RecordBatchStream for SeriesNormalizeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesNormalizeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let result = self.normalize_batch(&batch);
                Poll::Ready(Some(result))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl SeriesNormalizeStream {
    fn normalize_batch(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch.clone());
        }

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

        // Apply offset to timestamp column
        if self.offset != 0 {
            let ts_col = batch
                .column(self.ts_col_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("time index is not TimestampMillisecond".into())
                })?;

            let offset = self.offset;
            let adjusted: TimestampMillisecondArray =
                ts_col.iter().map(|opt| opt.map(|t| t + offset)).collect();
            columns[self.ts_col_index] = Arc::new(adjusted);
        }

        // Filter out NaN values if field column is specified
        if let Some(field_idx) = self.field_col_index {
            if let Some(float_arr) = batch
                .column(field_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
            {
                let mask: arrow_array::BooleanArray = float_arr
                    .iter()
                    .map(|opt| Some(opt.is_some_and(|v| !v.is_nan())))
                    .collect();

                let mut filtered_columns = Vec::with_capacity(columns.len());
                for col in &columns {
                    filtered_columns.push(arrow::compute::filter(col, &mask)?);
                }
                columns = filtered_columns;
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| e.into())
    }
}
