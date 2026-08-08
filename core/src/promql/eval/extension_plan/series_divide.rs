// Adapted from GreptimeDB — Apache License 2.0
// Splits interleaved Arrow batches into one batch per time series,
// grouped by label columns. Assumes input is sorted by tag columns then time.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion_expr::col;
use futures_util::{ready, Stream, StreamExt};

// ── Logical Node ──

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct SeriesDivide {
    pub tag_columns: Vec<String>,
    pub time_index_column: String,
    pub input: LogicalPlan,
}

impl SeriesDivide {
    pub fn new(tag_columns: Vec<String>, time_index_column: String, input: LogicalPlan) -> Self {
        Self {
            tag_columns,
            time_index_column,
            input,
        }
    }
}

impl UserDefinedLogicalNodeCore for SeriesDivide {
    fn name(&self) -> &str {
        "SeriesDivide"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.tag_columns
            .iter()
            .map(|c| col(c.as_str()))
            .chain(std::iter::once(col(self.time_index_column.as_str())))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            time_index_column: self.time_index_column.clone(),
            input: inputs.into_iter().next().unwrap(),
        })
    }
}

// ── Physical Exec ──

#[derive(Debug)]
pub struct SeriesDivideExec {
    pub tag_columns: Vec<String>,
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl SeriesDivideExec {
    pub fn new(tag_columns: Vec<String>, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input.properties().clone();
        Self {
            tag_columns,
            input,
            properties,
        }
    }
}

impl ExecutionPlan for SeriesDivideExec {
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
            self.tag_columns.clone(),
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
        let tag_indices: Vec<usize> = self
            .tag_columns
            .iter()
            .map(|tag| {
                schema
                    .column_with_name(tag)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| panic!("tag column not found: {tag}"))
            })
            .collect();
        Ok(Box::pin(SeriesDivideStream {
            tag_indices,
            buffer: vec![],
            schema,
            input,
            inspect_start: 0,
        }))
    }

    fn name(&self) -> &str {
        "SeriesDivideExec"
    }
}

impl DisplayAs for SeriesDivideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PromSeriesDivideExec: tags={:?}", self.tag_columns)
    }
}

// ── Stream ──

pub struct SeriesDivideStream {
    tag_indices: Vec<usize>,
    buffer: Vec<RecordBatch>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    inspect_start: usize,
}

impl RecordBatchStream for SeriesDivideStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesDivideStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if !self.buffer.is_empty() {
                let cut_at = match self.find_first_diff_row() {
                    Ok(v) => v,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                if let Some((batch_index, row_index)) = cut_at {
                    let first_half = self.buffer[batch_index].slice(0, row_index + 1);
                    let second_half = self.buffer[batch_index].slice(
                        row_index + 1,
                        self.buffer[batch_index].num_rows() - row_index - 1,
                    );
                    let result_batches: Vec<RecordBatch> = self
                        .buffer
                        .drain(0..batch_index)
                        .chain([first_half])
                        .collect();
                    if second_half.num_rows() > 0 {
                        self.buffer[0] = second_half;
                    } else {
                        self.buffer.remove(0);
                    }
                    let result = compute::concat_batches(&self.schema, &result_batches)?;
                    self.inspect_start = 0;
                    return Poll::Ready(Some(Ok(result)));
                } else {
                    let next_batch = ready!(self.as_mut().fetch_next_batch(cx)).transpose()?;
                    if let Some(next_batch) = next_batch {
                        if next_batch.num_rows() != 0 {
                            self.buffer.push(next_batch);
                        }
                        continue;
                    } else {
                        let result = compute::concat_batches(&self.schema, &self.buffer)?;
                        self.buffer.clear();
                        self.inspect_start = 0;
                        return Poll::Ready(Some(Ok(result)));
                    }
                }
            } else {
                let batch = match ready!(self.as_mut().fetch_next_batch(cx)) {
                    Some(Ok(batch)) => batch,
                    None => return Poll::Ready(None),
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                };
                self.buffer.push(batch);
            }
        }
    }
}

impl SeriesDivideStream {
    fn fetch_next_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DfResult<RecordBatch>>> {
        let this = self.get_mut();
        this.input.poll_next_unpin(cx)
    }

    /// Find the first row where tag values differ from the previous row.
    /// Returns `Some((batch_index, row_index))` of the last row before the boundary,
    /// or `None` if the entire buffer is one series.
    fn find_first_diff_row(&mut self) -> DfResult<Option<(usize, usize)>> {
        let num_batches = self.buffer.len();
        for batch_index in self.inspect_start..num_batches {
            let batch = &self.buffer[batch_index];
            let num_rows = batch.num_rows();
            let start_row = if batch_index == 0 { 1 } else { 0 };

            for row in start_row..num_rows {
                let prev_batch_idx;
                let prev_row;
                if row == 0 {
                    if batch_index == 0 {
                        continue;
                    }
                    prev_batch_idx = batch_index - 1;
                    prev_row = self.buffer[prev_batch_idx].num_rows() - 1;
                } else {
                    prev_batch_idx = batch_index;
                    prev_row = row - 1;
                }

                if !self.tags_equal(prev_batch_idx, prev_row, batch_index, row)? {
                    return Ok(Some(if row == 0 {
                        (prev_batch_idx, prev_row)
                    } else {
                        (batch_index, row - 1)
                    }));
                }
            }
        }
        self.inspect_start = if num_batches > 0 { num_batches - 1 } else { 0 };
        Ok(None)
    }

    fn tags_equal(
        &self,
        batch_a: usize,
        row_a: usize,
        batch_b: usize,
        row_b: usize,
    ) -> DfResult<bool> {
        let a = &self.buffer[batch_a];
        let b = &self.buffer[batch_b];
        for &col_idx in &self.tag_indices {
            let col_a = a.column(col_idx);
            let col_b = b.column(col_idx);
            let val_a = col_a
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|arr| arr.value(row_a));
            let val_b = col_b
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|arr| arr.value(row_b));
            if val_a != val_b {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
