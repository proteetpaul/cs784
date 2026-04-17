use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;

use crate::lip_build_exec::{Filter, LipBuildExec};
use crate::lip_filter_exec::LipFilterExec;

#[derive(Debug)]
pub struct LIPOptimizerRule {
    bloom_fp_rate: f32,
}

impl PhysicalOptimizerRule for LIPOptimizerRule {
    fn name(&self) -> &str {
        return "LIPOptimizerRule"
    }

    fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _config: &datafusion::config::ConfigOptions,
            ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !Self::validate_plan(&plan) {
            return Ok(plan);
        }
        self.transform_plan(&plan)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl LIPOptimizerRule {
    pub fn new(bloom_fp_rate: f32) -> Self {
        Self { bloom_fp_rate }
    }

    /// Validates that the plan contains at least one right-deep join chain
    /// suitable for LIP. Requirements:
    /// - More than 1 join in the chain
    /// - All joins are right-deep (no joins on the build/left side)
    /// - Each build-side (dimension) table is smaller than the probe (fact) table
    fn validate_plan(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.as_any().is::<HashJoinExec>() {
            if let Some((build_sides, probe_side)) = Self::extract_right_deep_chain(plan) {
                if build_sides.len() > 1 && Self::check_sizes(&build_sides, &probe_side) {
                    return true;
                }
            }
        }

        plan.children().iter().any(|child| Self::validate_plan(child))
    }

    /// Walk a right-deep HashJoinExec chain starting at `plan`.
    ///
    /// Returns `Some((build_sides, probe_side))` where `build_sides` are the
    /// left (dimension) inputs of each join and `probe_side` is the right
    /// input of the deepest join (the fact table).
    ///
    /// Returns `None` if any build side contains a HashJoinExec (not right-deep).
    fn extract_right_deep_chain(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<(Vec<Arc<dyn ExecutionPlan>>, Arc<dyn ExecutionPlan>)> {
        let mut build_sides: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut current = Arc::clone(plan);

        loop {
            let Some(hash_join) = current.as_any().downcast_ref::<HashJoinExec>() else {
                break;
            };

            let left = Arc::clone(hash_join.left());
            let right = Arc::clone(hash_join.right());

            if Self::contains_join(&left) {
                return None;
            }

            build_sides.push(left);
            current = right;
        }

        if build_sides.is_empty() {
            return None;
        }

        Some((build_sides, current))
    }

    /// Returns true if every build-side plan has fewer estimated rows than the
    /// probe-side plan. When statistics are unavailable for any input, the
    /// check is skipped (assumed valid).
    fn check_sizes(
        build_sides: &[Arc<dyn ExecutionPlan>],
        probe_side: &Arc<dyn ExecutionPlan>,
    ) -> bool {
        let probe_rows = probe_side
            .partition_statistics(None)
            .ok()
            .and_then(|s| s.num_rows.get_value().copied());

        let Some(probe_rows) = probe_rows else {
            return true;
        };

        for build in build_sides {
            let build_rows = build
                .partition_statistics(None)
                .ok()
                .and_then(|s| s.num_rows.get_value().copied());

            if let Some(build_rows) = build_rows {
                if build_rows >= probe_rows {
                    return false;
                }
            }
        }

        true
    }

    fn contains_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let mut found_join = false;
        let _ = plan.apply(|node| {
            if node.as_any().is::<HashJoinExec>() {
                found_join = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });
    
        found_join
    }

    /// Recursively walk the plan tree.  When a right-deep HashJoinExec
    /// chain is found, wrap every build-side (left) input with a
    /// `LipBuildExec` that populates a Bloom filter on the join key.
    fn transform_plan(&self, plan: &Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if plan.as_any().is::<HashJoinExec>() {
            if Self::extract_right_deep_chain(plan).is_some() {
                return self.transform_chain(plan)
            }
        }

        let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
            .children()
            .iter()
            .map(|c| self.transform_plan(c).unwrap())
            .collect();

        if new_children.is_empty() {
            return Ok(Arc::clone(plan));
        }

        Arc::clone(plan).with_new_children(new_children)
    }

    /// Transform a right-deep HashJoinExec chain by wrapping each
    /// build-side input with a `LipBuildExec`.
    fn transform_chain(&self, plan: &Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut joins: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut current = Arc::clone(plan);
        let mut left_children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut filters = Vec::<Filter>::new();

        // Traverse to the probe-side table
        while current.as_any().is::<HashJoinExec>() {
            let hj = current.as_any().downcast_ref::<HashJoinExec>().unwrap();
            let left = self.transform_plan(hj.left())?;
            let build_key_column = Self::build_key_column(hj);
            let probe_key_column = Self::probe_key_column(hj);
            let expected_rows = Self::estimate_rows(&left);
            let filter = Filter::new(probe_key_column.name().to_string(), self.bloom_fp_rate, expected_rows);

            let wrapped_left: Arc<dyn ExecutionPlan> =
                if let Some(coalesce) = left.as_any().downcast_ref::<CoalescePartitionsExec>() {
                    // If the immediate left child is a CoalescePartitionExec node, it will prevent us from parallelizing the creation of
                    // the bloom filter. So we push the LipBuildExec below this node.
                    let lip_inner = Arc::new(LipBuildExec::new(
                        Arc::clone(coalesce.input()),
                        build_key_column.clone(),
                        &filter.bloom,
                    ));
                    Arc::clone(&left).with_new_children(vec![lip_inner])?
                } else {
                    Arc::new(LipBuildExec::new(left, build_key_column, &filter.bloom))
                };
            filters.push(filter);
            left_children.push(wrapped_left);

            joins.push(Arc::clone(&current));
            current = Arc::clone(hj.right());
        }

        let mut right_child = self.transform_plan(&current)?;
        right_child = Arc::new(LipFilterExec::new(&right_child, filters))
            .with_new_children(vec![right_child])?;

        // Rebuild the chain bottom-up: each join gets its own wrapped build side from `left_children`.
        for (join_plan, wrapped_left) in joins.iter().rev().zip(left_children.iter().rev()) {
            right_child = Arc::clone(join_plan)
                .with_new_children(vec![Arc::clone(wrapped_left), right_child])?;
        }

        Ok(right_child)
    }

    /// Extract the build-side join-key column index from the first
    /// equijoin condition of a `HashJoinExec`.
    fn build_key_column(hj: &HashJoinExec) -> Column {
        hj.on()[0]
            .0
            .as_any()
            .downcast_ref::<Column>()
            .expect("build-side join key should be a Column expression")
            .clone()
    }

    /// Extract the probe-side join-key column index from the first
    /// equijoin condition of a `HashJoinExec`.
    fn probe_key_column(hj: &HashJoinExec) -> Column {
        hj.on()[0]
            .1
            .as_any()
            .downcast_ref::<Column>()
            .expect("build-side join key should be a Column expression")
            .clone()
    }

    fn estimate_rows(plan: &Arc<dyn ExecutionPlan>) -> u32 {
        plan.partition_statistics(None)
            .ok()
            .and_then(|s| s.num_rows.get_value().copied())
            .unwrap_or(10_000) as u32
    }
}