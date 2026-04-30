use std::{
    cmp::min,
    hash::Hash,
    sync::Arc,
    task::{Context, Poll},
};

use bloom::ASMS;
use datafusion::common::Result;
use datafusion::{
    arrow::{
        array::{Array, AsArray, RecordBatch, UInt64Array},
        compute::take_arrays,
        datatypes::{
            ArrowPrimitiveType, DataType, Date32Type, Date64Type, Int16Type, Int32Type, Int64Type,
            Int8Type, SchemaRef, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
    },
    execution::SendableRecordBatchStream,
    physical_plan::{
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time},
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::{Stream, StreamExt as _};

use crate::lip_build_exec::{Filter, LipBloomFilter};

/// Execution node responsible for pre-filtering the probe-side input
/// in a right-deep join tree using LIP. The input is filtered using bloom
/// filters and the result is passed to the HashJoinExec node
pub struct LipFilterExec {
    child: Arc<dyn ExecutionPlan>,
    filters: Arc<Vec<Filter>>,
    /// When true, reorder Bloom probes by estimated selectivity (LIP paper). When false, use join/planner filter order.
    adaptive_filter_reordering: bool,
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for LipFilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let filter_keys = self.filters.keys();
        f.debug_struct("LipFilterExec")
            .field("child", &self.child)
            // .field("filter_key_count", &filter_keys)
            .finish()
    }
}

impl DisplayAs for LipFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LipFilterExec")
    }
}

impl LipFilterExec {
    pub fn new(
        child: &Arc<dyn ExecutionPlan>,
        filters: Vec<Filter>,
        adaptive_filter_reordering: bool,
    ) -> LipFilterExec {
        LipFilterExec {
            child: child.clone(),
            filters: Arc::new(filters),
            adaptive_filter_reordering,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for LipFilterExec {
    fn name(&self) -> &str {
        "LipFilterExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &std::sync::Arc<datafusion::physical_plan::PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LipFilterExec {
            child: children[0].clone(),
            filters: Arc::clone(&self.filters),
            adaptive_filter_reordering: self.adaptive_filter_reordering,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input_rows = MetricBuilder::new(&self.metrics).counter("input_rows", partition);
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let input = self.child.execute(partition, context)?;
        let schema = input.schema();
        let num_filters = self.filters.len();
        let stream = LipFilterStream {
            child_stream: input,
            schema,
            filters: self.filters.clone(),
            adaptive_filter_reordering: self.adaptive_filter_reordering,
            count_map: vec![0; num_filters],
            miss_map: vec![0; num_filters],
            filter_ordering: (0..num_filters).collect(),
            input_rows,
            output_rows,
            elapsed_compute,
        };
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct LipFilterStream {
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    filters: Arc<Vec<Filter>>,
    adaptive_filter_reordering: bool,
    count_map: Vec<usize>,
    miss_map: Vec<usize>,
    filter_ordering: Vec<usize>,
    input_rows: Count,
    output_rows: Count,
    elapsed_compute: Time,
}

impl LipFilterStream {
    fn apply_one_filter(
        &mut self,
        batch: &RecordBatch,
        probe_indexes: &[usize],
        filter_idx: usize,
    ) -> Vec<usize> {
        let filter = &self.filters[filter_idx];
        let (column_idx, field) = self
            .schema
            .column_with_name(&filter.column_name)
            .unwrap_or_else(|| panic!("Invalid column name: {}", filter.column_name));
        let bloom_filter = filter.bloom.read().expect("Couldn't read bloom filter");
        let filtered_indexes: Vec<usize> = match field.data_type() {
            DataType::Int8 => probe_filter_using_column::<Int8Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::Int16 => probe_filter_using_column::<Int16Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::Int32 => probe_filter_using_column::<Int32Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::Int64 => probe_filter_using_column::<Int64Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::UInt8 => probe_filter_using_column::<UInt8Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::UInt16 => probe_filter_using_column::<UInt16Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::UInt32 => probe_filter_using_column::<UInt32Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::UInt64 => probe_filter_using_column::<UInt64Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::Date32 => probe_filter_using_column::<Date32Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            DataType::Date64 => probe_filter_using_column::<Date64Type>(
                &bloom_filter,
                probe_indexes,
                batch,
                column_idx,
            ),
            other => panic!("Unsupported datatype: {}", other),
        };

        self.count_map[filter_idx] += probe_indexes.len();
        self.miss_map[filter_idx] += probe_indexes.len() - filtered_indexes.len();
        filtered_indexes
    }

    /// When `adaptive_filter_reordering` is true, implements the adaptive reordering algorithm
    /// from the LIP paper (statistics and filter ordering persist across batches).
    fn filter_batch_with_adaptive_reordering(&mut self, batch: &RecordBatch) -> RecordBatch {
        let mut batch_size = 64;
        let mut loc = 0;
        let mut result_indexes = Vec::with_capacity(batch.num_rows());

        while loc < batch.num_rows() {
            let mut probe_indexes: Vec<usize> =
                (loc..min(loc + batch_size, batch.num_rows())).collect();
            if self.adaptive_filter_reordering {
                for filter_idx in self.filter_ordering.clone() {
                    probe_indexes = self.apply_one_filter(batch, &probe_indexes, filter_idx);
                }
                let count_map = &self.count_map;
                let miss_map = &self.miss_map;
                self.filter_ordering.sort_by(|a, b| {
                    let (a, b) = (*a, *b);
                    let lhs = miss_map[a] * count_map[b].max(1);
                    let rhs = miss_map[b] * count_map[a].max(1);
                    rhs.cmp(&lhs)
                });
            } else {
                for filter_idx in 0..self.filters.len() {
                    probe_indexes = self.apply_one_filter(batch, &probe_indexes, filter_idx);
                }
            }
            result_indexes.extend(probe_indexes);
            loc += batch_size;
            batch_size *= 2;
        }
        filter_record_batch(batch, &result_indexes)
    }
}

impl Stream for LipFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.child_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.input_rows.add(batch.num_rows());
                let start = std::time::Instant::now();
                let filtered = self.filter_batch_with_adaptive_reordering(&batch);
                self.elapsed_compute.add_elapsed(start);
                self.output_rows.add(filtered.num_rows());
                Poll::Ready(Some(Ok(filtered)))
            }
            other => other,
        }
    }
}

impl datafusion::execution::RecordBatchStream for LipFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Keep rows whose keys may be in `filter` (Bloom `contains`), or null keys (never inserted, passed through).
fn probe_filter_using_column<T>(
    filter: &LipBloomFilter,
    probe_indexes: &[usize],
    batch: &RecordBatch,
    column_idx: usize,
) -> Vec<usize>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    let arr = batch.column(column_idx).as_primitive::<T>();
    let mut out = Vec::with_capacity(probe_indexes.len());
    for &idx in probe_indexes {
        if arr.is_null(idx) {
            out.push(idx);
            continue;
        }
        let key = arr.value(idx);
        if filter.contains(&key) {
            out.push(idx);
        }
    }
    out
}

fn filter_record_batch(batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
    if indices.is_empty() {
        return RecordBatch::new_empty(batch.schema());
    }
    let idx = UInt64Array::from_iter_values(indices.iter().map(|&i| i as u64));
    let columns = take_arrays(batch.columns(), &idx, None).expect("take_arrays");
    RecordBatch::try_new(batch.schema(), columns).expect("RecordBatch::try_new")
}
