//! Physical rule: keep SSB Q3 **dimension** tables on the **build (left)** side of each
//! [`HashJoinExec`] and the fact (`lo_*`) on the **probe (right)**. If the fact side is on
//! the left, call [`HashJoinExec::swap_inputs`]. This must run **before** `EnforceDistribution`
//! (see DataFusion `swap_inputs` documentation).

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::common::Result;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::{CastExpr, Column, TryCastExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;

/// Coerces inner hash joins so SSB fact keys (`lo_*`) are not on the build (left) side.
#[derive(Debug, Default)]
pub struct CoerceSsbHashJoinBuildSide;

impl PhysicalOptimizerRule for CoerceSsbHashJoinBuildSide {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            if *hj.join_type() != JoinType::Inner {
                return Ok(Transformed::no(node));
            }

            if should_swap(hj) {
                return Ok(Transformed::yes(hj.swap_inputs(*hj.partition_mode())?));
            }
            Ok(Transformed::no(node))
        })
        .data()
    }

    fn name(&self) -> &str {
        "coerce_ssb_hash_join_build_side"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn should_swap(hj: &HashJoinExec) -> bool {
    if let Some((l_name, r_name)) = first_join_key_column_names(hj) {
        let left_fact = l_name.starts_with("lo_");
        let right_fact = r_name.starts_with("lo_");
        if left_fact && !right_fact {
            return true;
        }
        if left_fact || right_fact {
            return false;
        }
    }

    let left = hj.left();
    let right = hj.right();
    subtree_contains_hash_join(left) && !subtree_contains_hash_join(right)
}

fn first_join_key_column_names(hj: &HashJoinExec) -> Option<(String, String)> {
    let (l, r) = hj.on().first()?;
    let l = peel_to_column_name(l)?;
    let r = peel_to_column_name(r)?;
    Some((l, r))
}

fn peel_to_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    let mut current: &Arc<dyn PhysicalExpr> = expr;
    loop {
        if let Some(c) = current.as_any().downcast_ref::<Column>() {
            return Some(c.name().to_string());
        }
        if let Some(cast) = current.as_any().downcast_ref::<CastExpr>() {
            current = cast.expr();
            continue;
        }
        if let Some(tc) = current.as_any().downcast_ref::<TryCastExpr>() {
            current = tc.expr();
            continue;
        }
        return None;
    }
}

fn subtree_contains_hash_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if node.as_any().is::<HashJoinExec>() {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}
