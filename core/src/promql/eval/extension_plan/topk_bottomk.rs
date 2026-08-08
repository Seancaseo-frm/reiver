use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute;
use arrow_array::{
    Array, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::SchemaRef;
use datafusion::common::DFSchemaRef;
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
pub struct TopkBottomk {
    pub k: usize,
    pub is_topk: bool,
    pub partition_columns: Vec<String>,
    pub input: LogicalPlan,
}

impl TopkBottomk {
    pub fn new(
        k: usize,
        is_topk: bool,
        partition_columns: Vec<String>,
        input: LogicalPlan,
    ) -> Self {
        Self {
            k,
            is_topk,
            partition_columns,
            input,
        }
    }
}

impl UserDefinedLogicalNodeCore for TopkBottomk {
    fn name(&self) -> &str {
        "TopkBottomk"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![col("unix_milli"), col("value")]
            .into_iter()
            .chain(self.partition_columns.iter().map(|c| col(c.as_str())))
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let op = if self.is_topk { "topk" } else { "bottomk" };
        write!(
            f,
            "Prom{op}: k={}, partitions={:?}",
            self.k, self.partition_columns
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(Self {
            k: self.k,
            is_topk: self.is_topk,
            partition_columns: self.partition_columns.clone(),
            input: inputs.into_iter().next().unwrap(),
        })
    }
}

// ── Physical Exec ──

#[derive(Debug)]
pub struct TopkBottomkExec {
    pub k: usize,
    pub is_topk: bool,
    pub partition_columns: Vec<String>,
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl TopkBottomkExec {
    pub fn new(
        k: usize,
        is_topk: bool,
        partition_columns: Vec<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = input.properties().clone();
        Self {
            k,
            is_topk,
            partition_columns,
            input,
            properties,
        }
    }
}

impl ExecutionPlan for TopkBottomkExec {
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
            self.k,
            self.is_topk,
            self.partition_columns.clone(),
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
        Ok(Box::pin(TopkBottomkStream {
            k: self.k,
            is_topk: self.is_topk,
            partition_columns: self.partition_columns.clone(),
            schema,
            input,
            batches: Vec::new(),
            done: false,
        }))
    }

    fn name(&self) -> &str {
        "TopkBottomkExec"
    }
}

impl DisplayAs for TopkBottomkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let op = if self.is_topk { "topk" } else { "bottomk" };
        write!(
            f,
            "Prom{op}Exec: k={}, partitions={:?}",
            self.k, self.partition_columns
        )
    }
}

// ── Stream ──

/// Collects all input batches, then for each partition group (defined by
/// timestamp + partition columns), keeps only the top/bottom K rows by value.
pub struct TopkBottomkStream {
    k: usize,
    is_topk: bool,
    partition_columns: Vec<String>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    done: bool,
}

impl RecordBatchStream for TopkBottomkStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct RowRef {
    value: f64,
    partition_key: Vec<String>,
}

impl TopkBottomkStream {
    fn process_batches(&self, batches: &[RecordBatch]) -> DfResult<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let combined = compute::concat_batches(&self.schema, batches)?;
        if combined.num_rows() == 0 {
            return Ok(combined);
        }

        let ts_idx = self
            .schema
            .column_with_name("unix_milli")
            .map(|(i, _)| i)
            .unwrap_or(0);
        let val_idx = self
            .schema
            .column_with_name("value")
            .map(|(i, _)| i)
            .unwrap_or(1);

        let partition_indices: Vec<usize> = std::iter::once(ts_idx)
            .chain(self.partition_columns.iter().filter_map(|name| {
                self.schema.column_with_name(name).map(|(i, _)| i)
            }))
            .collect();

        let val_array = combined
            .column(val_idx)
            .as_any()
            .downcast_ref::<Float64Array>();

        let num_rows = combined.num_rows();
        let mut rows: Vec<RowRef> = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let value = val_array.map(|a| a.value(row)).unwrap_or(0.0);

            let mut partition_key = Vec::with_capacity(partition_indices.len());
            for &col_idx in &partition_indices {
                let col = combined.column(col_idx);
                let val = if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    arr.value(row).to_string()
                } else if let Some(arr) =
                    col.as_any().downcast_ref::<TimestampMillisecondArray>()
                {
                    arr.value(row).to_string()
                } else {
                    String::new()
                };
                partition_key.push(val);
            }

            rows.push(RowRef {
                value,
                partition_key,
            });
        }

        // Group by partition key, then within each group sort by value and
        // take top/bottom K.
        use std::collections::BTreeMap;
        let mut groups: BTreeMap<Vec<String>, Vec<usize>> = BTreeMap::new();
        for (i, row) in rows.iter().enumerate() {
            groups
                .entry(row.partition_key.clone())
                .or_default()
                .push(i);
        }

        let mut selected_indices: Vec<usize> = Vec::new();
        for (_key, mut indices) in groups {
            indices.sort_by(|&a, &b| {
                let va = rows[a].value;
                let vb = rows[b].value;
                if self.is_topk {
                    vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            selected_indices.extend(indices.into_iter().take(self.k));
        }

        selected_indices.sort_unstable();

        if selected_indices.len() == num_rows {
            return Ok(combined);
        }

        let indices = arrow_array::UInt32Array::from(
            selected_indices
                .iter()
                .map(|&i| i as u32)
                .collect::<Vec<_>>(),
        );

        let columns: Vec<Arc<dyn Array>> = (0..combined.num_columns())
            .map(|col_idx| arrow::compute::take(combined.column(col_idx), &indices, None))
            .collect::<Result<_, _>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for TopkBottomkStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        self.batches.push(batch);
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    self.done = true;
                    let batches = std::mem::take(&mut self.batches);
                    if batches.is_empty() {
                        return Poll::Ready(None);
                    }
                    return Poll::Ready(Some(self.process_batches(&batches)));
                }
            }
        }
    }
}
