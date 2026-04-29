use std::{cmp::min, hash::Hash, sync::Arc, task::{Context, Poll}};

use bloom::{ASMS, BloomFilter};
use datafusion::{arrow::{array::{Array, AsArray, RecordBatch, UInt64Array}, compute::take_arrays, datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, SchemaRef, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
}}, execution::SendableRecordBatchStream, physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time}}};
use datafusion::common::Result;
use futures::{Stream, StreamExt as _};

use crate::lip_build_exec::Filter;

/// Execution node responsible for pre-filtering the probe-side input
/// in a right-deep join tree using LIP. The input is filtered using bloom
/// filters and the result is passed to the HashJoinExec node 
pub struct LipFilterExec {
    child: Arc<dyn ExecutionPlan>,
    filters: Arc<Vec<Filter>>,
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
    pub fn new(child: &Arc<dyn ExecutionPlan>, filters: Vec<Filter>) -> LipFilterExec {
        LipFilterExec {
            child: child.clone(),
            filters: Arc::new(filters),
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
        let stream = LipFilterStream {
            child_stream: input,
            schema,
            filters: self.filters.clone(),
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
    input_rows: Count,
    output_rows: Count,
    elapsed_compute: Time,
}

impl LipFilterStream {
    /// Implements the adaptive reordering algorithm from the LIP paper
    fn filter_batch_with_adaptive_reordering(&self, batch: &RecordBatch) -> RecordBatch {
        let mut batch_size = 64;
        let mut loc = 0;
        let mut count_map: Vec<usize> = vec![0; self.filters.len()];
        let mut miss_map: Vec<usize> = vec![0; self.filters.len()];
        let mut filter_ordering: Vec<usize> = (0..self.filters.len()).map(usize::from).collect();
        let mut result_indexes = Vec::new();

        while loc < batch.num_rows() {
            let mut probe_indexes: Vec<usize> = (loc..min(loc+batch_size, batch.num_rows())).map(usize::from).collect();
            for filter_idx in &filter_ordering {
                let filter = &self.filters[*filter_idx];
                let (column_idx, field) = self.schema.column_with_name(&filter.column_name).unwrap_or_else(|| {
                    panic!("Invalid column name: {}", filter.column_name);
                });
                let bloom_filter = filter.bloom.read().expect("Couldn't read bloom filter");
                let filtered_indexes: Vec<usize> = match field.data_type() {
                    DataType::Int8 => probe_filter_using_column::<Int8Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::Int16 => probe_filter_using_column::<Int16Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::Int32 => probe_filter_using_column::<Int32Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::Int64 => probe_filter_using_column::<Int64Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::UInt8 => probe_filter_using_column::<UInt8Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::UInt16 => probe_filter_using_column::<UInt16Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::UInt32 => probe_filter_using_column::<UInt32Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::UInt64 => probe_filter_using_column::<UInt64Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::Date32 => probe_filter_using_column::<Date32Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    DataType::Date64 => probe_filter_using_column::<Date64Type>(&bloom_filter, &probe_indexes, batch, column_idx),
                    other => panic!("Unsupported datatype: {}", other),
                };
                
                count_map[*filter_idx] += probe_indexes.len();
                miss_map[*filter_idx] += probe_indexes.len() - filtered_indexes.len();
                probe_indexes = filtered_indexes;
            }
            // Add probe_indexes to result
            result_indexes.extend(probe_indexes);
            // Reorder filters according to their miss ratio (more selective filters first)
            filter_ordering.sort_by(|a, b| {
                let (a, b) = (*a, *b);
                // Compare miss_map/count_map without integer division (and avoid div-by-zero).
                let lhs = miss_map[a] * count_map[b].max(1);
                let rhs = miss_map[b] * count_map[a].max(1);
                return lhs.cmp(&rhs)
            });
            loc += batch_size;
            batch_size *= 2;
        }
        let res = filter_record_batch(batch, &result_indexes);
        res
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
                let timer = self.elapsed_compute.timer();
                let filtered = self.filter_batch_with_adaptive_reordering(&batch);
                timer.done();
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
    filter: &BloomFilter,
    probe_indexes: &[usize],
    batch: &RecordBatch,
    column_idx: usize,
) -> Vec<usize>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    let arr = batch.column(column_idx).as_primitive::<T>();
    let mut out = Vec::new();
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
