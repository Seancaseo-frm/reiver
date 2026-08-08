// Adapted from GreptimeDB — Apache License 2.0
// Aligns instant vector samples to evaluation timestamps with Prometheus lookback semantics.
// For each evaluation timestamp, finds the most recent sample within the lookback window.

use std::any::Any;
use std::cmp::Ordering;
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
pub struct InstantManipulate {
    pub start: Millisecond,
    pub end: Millisecond,
    pub lookback_delta: Millisecond,
    pub interval: Millisecond,
    pub time_index_column: String,
    pub field_column: Option<String>,
    pub input: LogicalPlan,
}

impl InstantManipulate {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        time_index_column: String,
        field_column: Option<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            start,
            end,
            lookback_delta,
            interval,
            time_index_column,
            field_column,
            input,
        }
    }

    pub const fn name() -> &'static str {
        "InstantManipulate"
    }
}

impl UserDefinedLogicalNodeCore for InstantManipulate {
    fn name(&self) -> &str {
        Self::name()
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
            "PromInstantManipulate: range=[{}..{}], lookback=[{}], interval=[{}]",
            self.start, self.end, self.lookback_delta, self.interval
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        Ok(Self::new(
            self.start,
            self.end,
            self.lookback_delta,
            self.interval,
            self.time_index_column.clone(),
            self.field_column.clone(),
            inputs.into_iter().next().unwrap(),
        ))
    }
}

// ── Physical Exec ──

#[derive(Debug)]
pub struct InstantManipulateExec {
    pub start: Millisecond,
    pub end: Millisecond,
    pub lookback_delta: Millisecond,
    pub interval: Millisecond,
    pub time_index_column: String,
    pub field_column: Option<String>,
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl InstantManipulateExec {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        lookback_delta: Millisecond,
        interval: Millisecond,
        time_index_column: String,
        field_column: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            start,
            end,
            lookback_delta,
            interval,
            time_index_column,
            field_column,
            input,
            properties,
        }
    }
}

impl ExecutionPlan for InstantManipulateExec {
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
            self.start,
            self.end,
            self.lookback_delta,
            self.interval,
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

        Ok(Box::pin(InstantManipulateStream {
            start: self.start,
            end: self.end,
            lookback_delta: self.lookback_delta,
            interval: self.interval,
            ts_col_index,
            field_col_index,
            schema,
            input,
        }))
    }

    fn name(&self) -> &str {
        "InstantManipulateExec"
    }
}

impl DisplayAs for InstantManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromInstantManipulateExec: range=[{}..{}], lookback=[{}], interval=[{}]",
            self.start, self.end, self.lookback_delta, self.interval
        )
    }
}

// ── Stream ──

pub struct InstantManipulateStream {
    start: Millisecond,
    end: Millisecond,
    lookback_delta: Millisecond,
    interval: Millisecond,
    ts_col_index: usize,
    field_col_index: Option<usize>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl RecordBatchStream for InstantManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for InstantManipulateStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let result = self.align_batch(&batch);
                match result {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl InstantManipulateStream {
    fn align_batch(&self, batch: &RecordBatch) -> DfResult<Option<RecordBatch>> {
        let ts_column = batch
            .column(self.ts_col_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("time index is not TimestampMillisecond".into())
            })?;

        let num_rows = ts_column.len();
        if num_rows == 0 {
            return Ok(None);
        }

        let ts_values = ts_column.values();

        // For each evaluation timestamp, find the best matching sample
        let mut take_indices: Vec<Option<usize>> = Vec::new();
        let mut eval_timestamps: Vec<i64> = Vec::new();
        let mut aligned_ts = self.start;
        let mut search_start = 0;

        while aligned_ts <= self.end {
            let lookback_start = aligned_ts - self.lookback_delta;

            // Find the rightmost sample <= aligned_ts and >= lookback_start.
            // A stale NaN marker invalidates any prior best — if the most recent
            // sample in the window is stale, no output is produced (Prometheus semantics).
            let mut best = None;
            for i in search_start..num_rows {
                let sample_ts = ts_values[i];
                if sample_ts > aligned_ts {
                    break;
                }
                if sample_ts >= lookback_start {
                    if let Some(field_idx) = self.field_col_index {
                        if let Some(float_arr) = batch
                            .column(field_idx)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                        {
                            if float_arr.is_null(i) || float_arr.value(i).is_nan() {
                                best = None;
                                continue;
                            }
                        }
                    }
                    best = Some(i);
                }
            }

            if best.is_some() {
                take_indices.push(best);
                eval_timestamps.push(aligned_ts);
            }

            aligned_ts += self.interval;
        }

        if eval_timestamps.is_empty() {
            return Ok(None);
        }

        // Build output batch
        let indices = arrow_array::UInt32Array::from(
            take_indices
                .iter()
                .map(|opt| opt.map(|i| i as u32))
                .collect::<Vec<_>>(),
        );

        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            if col_idx == self.ts_col_index {
                let aligned_array =
                    TimestampMillisecondArray::from_iter(eval_timestamps.iter().map(|&t| Some(t)));
                output_columns.push(Arc::new(aligned_array));
            } else {
                let taken = arrow::compute::take(batch.column(col_idx), &indices, None)?;
                output_columns.push(taken);
            }
        }

        let result = RecordBatch::try_new(self.schema.clone(), output_columns)?;
        Ok(Some(result))
    }
}
