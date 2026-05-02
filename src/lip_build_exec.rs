use std::sync::{Arc, LazyLock, RwLock};

use ahash::RandomState;
use bloom::{BloomFilter, ASMS};
use datafusion::arrow::array::{
    Array, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::{
    metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time},
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::stream::StreamExt;
use futures::Stream;

use std::task::{Context, Poll};

pub type LipBloomFilter = BloomFilter<RandomState, RandomState>;

// Always use the same independent hash builders for build-side inserts and probe-side lookups.
static HASH_BUILDER_ONE: LazyLock<RandomState> =
    LazyLock::new(|| RandomState::with_seeds('L' as u64, 'I' as u64, 'P' as u64, '1' as u64));
static HASH_BUILDER_TWO: LazyLock<RandomState> =
    LazyLock::new(|| RandomState::with_seeds('L' as u64, 'I' as u64, 'P' as u64, '2' as u64));

/// Encapsulates a column name and its associated global bloom filter
pub struct Filter {
    pub column_name: String,
    pub bloom: Arc<RwLock<LipBloomFilter>>,
}

impl Filter {
    pub fn new(column_name: String, fp_rate: f32, expected_num_items: u32) -> Filter {
        let bloom = BloomFilter::with_rate_and_hashers(
            fp_rate,
            expected_num_items,
            (*HASH_BUILDER_ONE).clone(),
            (*HASH_BUILDER_TWO).clone(),
        );
        eprintln!(
            "[LIPOptimizerRule] Bloom filter column={}: num_hashes={}, num_bits={}, estimated_elements={}",
            column_name,
            bloom.num_hashes(),
            bloom.num_bits(),
            expected_num_items
        );
        Filter {
            column_name,
            bloom: Arc::new(RwLock::new(bloom)),
        }
    }
}

/// Node in the physical execution plan that is responsible for building bloom filters
/// on the build-side inputs of a right-deep join tree. The rows from the input are passed
/// unchanged to the HashJoinExec operator. The bloom filters are propagated to LipFilterExec
/// using shared memory.
pub struct LipBuildExec {
    child: Arc<dyn ExecutionPlan>,
    key_column: Column,
    bloom_filter: Arc<RwLock<LipBloomFilter>>,
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for LipBuildExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LipBuildExec")
            .field("child", &self.child)
            .field("key_column", &self.key_column)
            .finish()
    }
}

impl LipBuildExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        key_column: Column,
        global_filter: &Arc<RwLock<LipBloomFilter>>,
    ) -> Self {
        Self {
            child,
            key_column,
            bloom_filter: global_filter.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for LipBuildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LipBuildExec: key_column={}", self.key_column)
    }
}

impl ExecutionPlan for LipBuildExec {
    fn name(&self) -> &str {
        "LipBuildExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LipBuildExec {
            child: children[0].clone(),
            key_column: self.key_column.clone(),
            bloom_filter: self.bloom_filter.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let input = self.child.execute(partition, context)?;
        let schema = input.schema();
        let stream = LipBuildStream {
            child_stream: input,
            global_filter: Arc::clone(&self.bloom_filter),
            key_column: self.key_column.clone(),
            schema,
            elapsed_compute,
        };
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct LipBuildStream {
    child_stream: SendableRecordBatchStream,
    global_filter: Arc<RwLock<LipBloomFilter>>,
    key_column: Column,
    schema: SchemaRef,
    elapsed_compute: Time,
}

impl LipBuildStream {
    fn insert_batch(&self, batch: &RecordBatch) {
        let key_array = batch.column(self.key_column.index());
        let mut filter = self.global_filter.write().unwrap();

        if let Some(arr) = key_array.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    filter.insert(&arr.value(i));
                }
            }
        } else if let Some(arr) = key_array.as_any().downcast_ref::<Int32Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    filter.insert(&arr.value(i));
                }
            }
        } else if let Some(arr) = key_array.as_any().downcast_ref::<Date32Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    filter.insert(&arr.value(i));
                }
            }
        } else if let Some(arr) = key_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    filter.insert(&arr.value(i));
                }
            }
        }
    }
}

impl Stream for LipBuildStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.child_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let timer = self.elapsed_compute.timer();
                self.insert_batch(&batch);
                timer.done();
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl datafusion::execution::RecordBatchStream for LipBuildStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
