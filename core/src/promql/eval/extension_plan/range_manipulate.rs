// Adapted from GreptimeDB — Apache License 2.0
// Creates sliding windows over time-series data for range functions (rate, increase, etc.).
// Converts timestamp and value columns into RangeArrays.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_expr::col;
use futures_util::{ready, Stream, StreamExt};

use crate::promql::eval::range_array::RangeArray;

pub type Millisecond = i64;

// ── Logical Node ──

#[derive(Debug, Hash)]
pub struct RangeManipulate {
    pub start: Millisecond,
    pub end: Millisecond,
    pub interval: Millisecond,
    pub range: Millisecond,
    pub time_index: String,
    pub field_columns: Vec<String>,
    pub input: LogicalPlan,
    pub output_schema: DFSchemaRef,
}

impl PartialEq for RangeManipulate {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start
            && self.end == other.end
            && self.interval == other.interval
            && self.range == other.range
            && self.time_index == other.time_index
            && self.field_columns == other.field_columns
            && self.input == other.input
    }
}

impl Eq for RangeManipulate {}

impl PartialOrd for RangeManipulate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.time_index.partial_cmp(&other.time_index)
    }
}

impl RangeManipulate {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        time_index: String,
        field_columns: Vec<String>,
        input: LogicalPlan,
    ) -> DfResult<Self> {
        let output_schema =
            Self::calculate_output_schema(input.schema(), &time_index, &field_columns)?;
        Ok(Self {
            start,
            end,
            interval,
            range,
            time_index,
            field_columns,
            input,
            output_schema,
        })
    }

    pub fn build_timestamp_range_name(time_index: &str) -> String {
        format!("{time_index}_range")
    }

    fn range_timestamp_name(&self) -> String {
        Self::build_timestamp_range_name(&self.time_index)
    }

    fn calculate_output_schema(
        input_schema: &DFSchemaRef,
        time_index: &str,
        field_columns: &[String],
    ) -> DfResult<DFSchemaRef> {
        let columns = input_schema.fields();
        let mut new_columns: Vec<(Option<datafusion::common::TableReference>, Arc<Field>)> =
            Vec::with_capacity(columns.len() + 1);
        for i in 0..columns.len() {
            let x = input_schema.qualified_field(i);
            new_columns.push((x.0.cloned(), Arc::new(x.1.clone())));
        }

        let ts_col_index = input_schema
            .index_of_column_by_name(None, time_index)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("time index column '{time_index}' not found"))
            })?;
        let ts_col_field = &columns[ts_col_index];
        let timestamp_range_field = Field::new(
            Self::build_timestamp_range_name(time_index),
            RangeArray::convert_field(ts_col_field).data_type().clone(),
            ts_col_field.is_nullable(),
        );
        new_columns.push((None, Arc::new(timestamp_range_field)));

        for name in field_columns {
            let index = input_schema
                .index_of_column_by_name(None, name)
                .ok_or_else(|| DataFusionError::Plan(format!("field column '{name}' not found")))?;
            new_columns[index] = (None, Arc::new(RangeArray::convert_field(&columns[index])));
        }

        Ok(Arc::new(DFSchema::new_with_metadata(
            new_columns,
            HashMap::new(),
        )?))
    }
}

impl UserDefinedLogicalNodeCore for RangeManipulate {
    fn name(&self) -> &str {
        "RangeManipulate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![col(self.time_index.as_str())]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromRangeManipulate: range=[{}..{}], interval=[{}], window=[{}]",
            self.start, self.end, self.interval, self.range
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        Self::new(
            self.start,
            self.end,
            self.interval,
            self.range,
            self.time_index.clone(),
            self.field_columns.clone(),
            inputs.into_iter().next().unwrap(),
        )
    }
}

// ── Physical Exec ──

#[derive(Debug)]
pub struct RangeManipulateExec {
    pub start: Millisecond,
    pub end: Millisecond,
    pub interval: Millisecond,
    pub range: Millisecond,
    pub time_index_column: String,
    pub time_range_column: String,
    pub field_columns: Vec<String>,
    pub input: Arc<dyn ExecutionPlan>,
    pub output_schema: SchemaRef,
    properties: PlanProperties,
}

impl RangeManipulateExec {
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        range: Millisecond,
        time_index_column: String,
        field_columns: Vec<String>,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        );
        Self {
            start,
            end,
            interval,
            range,
            time_index_column: time_index_column.clone(),
            time_range_column: RangeManipulate::build_timestamp_range_name(&time_index_column),
            field_columns,
            input,
            output_schema,
            properties,
        }
    }
}

impl ExecutionPlan for RangeManipulateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
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
            self.interval,
            self.range,
            self.time_index_column.clone(),
            self.field_columns.clone(),
            children[0].clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let input_schema = input.schema();
        let ts_col_index = input_schema
            .column_with_name(&self.time_index_column)
            .unwrap()
            .0;
        let field_col_indices: Vec<usize> = self
            .field_columns
            .iter()
            .map(|name| input_schema.column_with_name(name).unwrap().0)
            .collect();

        Ok(Box::pin(RangeManipulateStream {
            start: self.start,
            end: self.end,
            interval: self.interval,
            range: self.range,
            ts_col_index,
            field_col_indices,
            output_schema: self.output_schema.clone(),
            input,
        }))
    }

    fn name(&self) -> &str {
        "RangeManipulateExec"
    }
}

impl DisplayAs for RangeManipulateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PromRangeManipulateExec: range=[{}..{}], interval=[{}], window=[{}]",
            self.start, self.end, self.interval, self.range
        )
    }
}

// ── Stream ──

pub struct RangeManipulateStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    range: Millisecond,
    ts_col_index: usize,
    field_col_indices: Vec<usize>,
    output_schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl RecordBatchStream for RangeManipulateStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for RangeManipulateStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let result = self.manipulate_batch(&batch);
                match result {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        // empty result for this series, poll next
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

impl RangeManipulateStream {
    fn manipulate_batch(&self, batch: &RecordBatch) -> DfResult<Option<RecordBatch>> {
        let ts_column = batch
            .column(self.ts_col_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("time index column is not TimestampMillisecond".into())
            })?;

        let num_rows = ts_column.len();
        if num_rows == 0 {
            return Ok(None);
        }

        let ts_values = ts_column.values();

        // Build evaluation points and their windows
        let mut eval_ts_list = Vec::new();
        let mut ranges: Vec<(u32, u32)> = Vec::new();
        let mut aligned_ts = self.start;

        while aligned_ts <= self.end {
            let range_start = aligned_ts - self.range;
            let range_end = aligned_ts;

            // Binary search for the window bounds
            let left = ts_values.partition_point(|&t| t < range_start);
            let right = ts_values.partition_point(|&t| t <= range_end);

            if left < right {
                eval_ts_list.push(aligned_ts);
                ranges.push((left as u32, (right - left) as u32));
            }

            aligned_ts += self.interval;
        }

        if eval_ts_list.is_empty() {
            return Ok(None);
        }

        // Build output columns
        let num_output_rows = eval_ts_list.len();
        let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);

        for col_idx in 0..batch.num_columns() {
            if col_idx == self.ts_col_index {
                // Keep original timestamp column as evaluation timestamps
                let eval_ts_array =
                    TimestampMillisecondArray::from_iter(eval_ts_list.iter().map(|&t| Some(t)));
                output_columns.push(Arc::new(eval_ts_array));
            } else if self.field_col_indices.contains(&col_idx) {
                // Convert value columns to RangeArrays
                let values = batch.column(col_idx).clone();
                let range_array = RangeArray::from_ranges(values, ranges.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                output_columns.push(Arc::new(range_array.into_dict()));
            } else {
                // Tag columns: take first value from each range (they're all the same per series)
                let col = batch.column(col_idx);
                let first_indices: Vec<u32> = ranges.iter().map(|(off, _)| *off).collect();
                let indices = arrow_array::UInt32Array::from(first_indices);
                let taken = arrow::compute::take(col, &indices, None)?;
                output_columns.push(taken);
            }
        }

        // Append timestamp range column (RangeArray of timestamps)
        let ts_values_ref: ArrayRef = batch.column(self.ts_col_index).clone();
        let ts_range_array = RangeArray::from_ranges(ts_values_ref, ranges)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        output_columns.push(Arc::new(ts_range_array.into_dict()));

        let result = RecordBatch::try_new(self.output_schema.clone(), output_columns)?;
        Ok(Some(result))
    }
}
