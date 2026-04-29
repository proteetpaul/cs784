use std::hash::RandomState;
use std::sync::{Arc, LazyLock, RwLock};

use bloom::{ASMS, BloomFilter, Unionable};
use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time},
};
use futures::stream::StreamExt;
use futures::Stream;

use std::task::{Context, Poll};

// Always use the same hash builders for the bloom filters to support union operations
static HASH_BUILDER_ONE: LazyLock<RandomState> = LazyLock::new(RandomState::new);
static HASH_BUILDER_TWO: LazyLock<RandomState> = LazyLock::new(RandomState::new);

/// Encapsulates a column name and its associated global bloom filter
pub struct Filter {
    pub column_name: String,
    pub bloom: Arc<RwLock<BloomFilter>>,
}

impl Filter {
    pub fn new(column_name: String, fp_rate: f32, expected_num_items: u32) -> Filter {
        Filter {
            column_name,
            bloom: Arc::new(RwLock::new(BloomFilter::with_rate_and_hashers(
                fp_rate,
                expected_num_items,
                (*HASH_BUILDER_ONE).clone(),
                (*HASH_BUILDER_TWO).clone(),
            ))),
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
    bloom_filter: Arc<RwLock<BloomFilter>>,
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
        global_filter: &Arc<RwLock<BloomFilter>>,
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
        let filter = self.bloom_filter.read().expect("Unable to lock bloom filter");
        let num_bits = filter.num_bits();
        let num_hashes = filter.num_hashes();

        let stream = LipBuildStream {
            child_stream: input,
            global_filter: Arc::clone(&self.bloom_filter),
            key_column: self.key_column.clone(),
            schema,
            num_bits,
            num_hashes,
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
    global_filter: Arc<RwLock<BloomFilter>>,
    key_column: Column,
    schema: SchemaRef,
    num_bits: usize,
    num_hashes: u32,
    elapsed_compute: Time,
}

impl LipBuildStream {
    fn insert_batch(&self, batch: &RecordBatch) {
        let key_array = batch.column(self.key_column.index());
        // Each partition builds a local bloom filter in parallel, and finally combines this with the global filter
        // TODO(): Use a thread-local filter to avoid repeated initialization
        let mut local_filter = BloomFilter::with_size_and_hashers(
            self.num_bits, 
            self.num_hashes, 
            (*HASH_BUILDER_ONE).clone(),
            (*HASH_BUILDER_TWO).clone()
        );

        if let Some(arr) = key_array.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    local_filter.insert(&arr.value(i));
                }
            }
        } else if let Some(arr) = key_array.as_any().downcast_ref::<Int32Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    local_filter.insert(&arr.value(i));
                }
            }
        } else if let Some(arr) = key_array.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    local_filter.insert(&arr.value(i));
                }
            }
        }

        let mut filter = self.global_filter.write().unwrap();
        filter.union(&local_filter);
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
