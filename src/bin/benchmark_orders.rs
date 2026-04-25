//! SSB join-order micro-benchmark: run Q3.1–Q3.3 (`3!` permutations) and Q4.1–Q4.3 (`4!`
//! permutations). Each join extends a left-deep tree starting from `lineorder`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode,
};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::dataframe::DataFrame;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use lip_datafusion::optimizer_rule::LIPOptimizerRule;
use lip_datafusion::ssb::coerce_join_order::CoerceSsbHashJoinBuildSide;
use lip_datafusion::ssb::join_order_q3::{
    self, DIMENSION_JOIN_PERMUTATIONS, Q3Query, format_dim_order as format_dim_order_q3,
};
use lip_datafusion::ssb::join_order_q4::{
    Q4_DIMENSION_JOIN_PERMUTATIONS, Q4Query, build_q4_logical_plan, format_dim_order as format_dim_order_q4,
};
use lip_datafusion::ssb::load;
use pprof::ProfilerGuardBuilder;

#[derive(Parser)]
#[command(
    name = "benchmark_orders",
    about = "SSB Q3.1–Q3.3 and Q4.1–Q4.3: time each dimension-permutation join plan with optional LIP"
)]
struct Cli {
    /// Directory containing `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`, `lineorder.tbl`
    #[arg(long)]
    data_dir: PathBuf,

    /// Comma-separated query ids (3.1–3.3 and 4.1–4.3)
    #[arg(long, value_delimiter = ',')]
    queries: Option<Vec<String>>,

    /// Number of times to repeat each (query, join-order) pair (mean wall time reported)
    #[arg(long, default_value = "5")]
    iterations: usize,

    /// Enable lookahead information passing (LIP physical rule)
    #[arg(long, default_value_t = false)]
    lip: bool,

    #[arg(long, default_value_t = 0.01)]
    lip_fp_rate: f32,

    /// Print the optimized physical plan for each query and dimension join order
    #[arg(long, default_value_t = false)]
    explain_physical: bool,

    /// If set, write `flamegraph.svg` here for the first timed run only (first query in
    /// `--queries`, first dimension join order, first iteration).
    #[arg(long)]
    flamegraph_dir: Option<PathBuf>,
}

/// Replaces DataFusion's `JoinSelection` for this benchmark: `JoinSelection` resolves
/// `PartitionMode::Auto` (required before execute) but also swaps join sides from statistics.
/// We only perform the `Auto` → `Partitioned` step so plans stay runnable without reordering children.
#[derive(Debug)]
struct ResolveHashJoinAutoMode;

impl PhysicalOptimizerRule for ResolveHashJoinAutoMode {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
                if *hj.partition_mode() == PartitionMode::Auto {
                    let rebuilt = hj
                        .builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .build_exec()?;
                    return Ok(Transformed::yes(rebuilt));
                }
            }
            Ok(Transformed::no(node))
        })
        .data()
    }

    fn name(&self) -> &str {
        "resolve_hash_join_auto_mode"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn build_session(lip: bool, lip_fp_rate: f32) -> SessionContext {
    // Swap out `join_selection` only: keep all other default physical rules.
    let default_phys = PhysicalOptimizer::new();
    let rules: Vec<_> = default_phys
        .rules
        .into_iter()
        .map(|rule| {
            if rule.name() == "join_selection" {
                Arc::new(ResolveHashJoinAutoMode) as Arc<dyn PhysicalOptimizerRule + Send + Sync>
            } else {
                rule
            }
        })
        .collect();

    let coerce: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
        Arc::new(CoerceSsbHashJoinBuildSide::default());
    let mut rules_with_coerce = Vec::with_capacity(rules.len() + 1);
    for rule in rules {
        if rule.name() == "EnforceDistribution" {
            rules_with_coerce.push(Arc::clone(&coerce));
        }
        rules_with_coerce.push(rule);
    }

    let mut session_config = SessionConfig::new();
    // if lip {
    // LIP already injects Bloom semi-join style filtering; disable DataFusion join/topk/aggregate
    // dynamic filter pushdown to avoid stacking two mechanisms on the same plans.
    session_config = session_config.set_bool(
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        false,
    );
    // }

    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_physical_optimizer_rules(rules_with_coerce)
        .with_default_features();

    if lip {
        builder = builder.with_physical_optimizer_rule(Arc::new(LIPOptimizerRule::new(
            lip_fp_rate,
        )));
    }

    let state = builder.build();
    SessionContext::new_with_state(state)
}

fn mean_wall_ms(timings: &[Duration]) -> f64 {
    let n = timings.len() as f64;
    if n == 0.0 {
        return 0.0;
    }
    timings.iter().map(|d| d.as_secs_f64()).sum::<f64>() * 1000.0 / n
}

async fn run_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<(usize, Duration)> {
    let df = DataFrame::new(ctx.state().clone(), plan);
    let start = Instant::now();
    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((row_count, elapsed))
}

/// Runs `run_logical_plan` while optionally sampling stacks and writing an SVG flamegraph.
async fn run_logical_plan_with_optional_flamegraph(
    ctx: &SessionContext,
    plan: LogicalPlan,
    flamegraph_svg: Option<&Path>,
) -> Result<(usize, Duration)> {
    match flamegraph_svg {
        Some(svg_path) => {
            let guard = ProfilerGuardBuilder::default()
                .frequency(1000)
                .blocklist(&["libc", "libgcc", "libpthread", "vdso"])
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let timing = run_logical_plan(ctx, plan).await?;

            let report = guard
                .report()
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if report.data.is_empty() {
                log::warn!(
                    "CPU profiler captured no samples; flamegraph may be empty (try debug build or RUSTFLAGS='-C force-frame-pointers=yes')"
                );
            }

            let mut file = File::create(svg_path).map_err(DataFusionError::IoError)?;
            report
                .flamegraph(&mut file)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            log::info!("Wrote flamegraph to {}", svg_path.display());
            Ok(timing)
        }
        None => run_logical_plan(ctx, plan).await,
    }
}

async fn print_physical_plan_logical(
    ctx: &SessionContext,
    logical_plan: &LogicalPlan,
    label: &str,
) -> Result<()> {
    let df = DataFrame::new(ctx.state().clone(), logical_plan.clone());
    let plan = df.create_physical_plan().await?;
    log::info!(
        "--- Physical plan: {label} ---\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    if cli.iterations == 0 {
        return exec_err!("--iterations must be at least 1");
    }

    let query_ids: Vec<String> = cli.queries.unwrap_or_else(|| {
        vec![
            "3.1".to_string(),
            "3.2".to_string(),
            "3.3".to_string(),
        ]
    });

    for qid in &query_ids {
        if Q3Query::parse(qid).is_none() && Q4Query::parse(qid).is_none() {
            return exec_err!(
                "Unsupported query id {qid}: benchmark_orders supports 3.1–3.3 and 4.1–4.3"
            );
        }
    }

    log::info!(
        "Reading SSB .tbl files from {} into memory ...",
        cli.data_dir.display()
    );
    let ctx = build_session(cli.lip, cli.lip_fp_rate);
    load::register_ssb_tables(&ctx, &cli.data_dir).await?;

    log::info!(
        "Join-order counts: Q3 (3.1–3.3) {} permutations; Q4 (4.1–4.3) {} permutations.",
        DIMENSION_JOIN_PERMUTATIONS.len(),
        Q4_DIMENSION_JOIN_PERMUTATIONS.len()
    );
    log::info!(
        "LIP {}.\n",
        if cli.lip {
            format!("on, fp_rate={}", cli.lip_fp_rate)
        } else {
            "off".to_string()
        }
    );

    if let Some(dir) = &cli.flamegraph_dir {
        fs::create_dir_all(dir).map_err(DataFusionError::IoError)?;
        log::info!(
            "Flamegraph output: {} (first query, first join order, iteration 0 only)",
            dir.join("flamegraph.svg").display()
        );
    }

    log::info!(
        "{:<6} {:<48} {:>10} {:>12}",
        "Query", "dim_order", "Rows", "Avg (ms)"
    );
    log::info!("{}", "-".repeat(80));

    for (q_idx, qid) in query_ids.iter().enumerate() {
        let mut expected_rows: Option<usize> = None;

        if let Some(q) = Q3Query::parse(qid) {
            for (d_idx, dim_order) in DIMENSION_JOIN_PERMUTATIONS.iter().enumerate() {
                let order_label = format_dim_order_q3(dim_order);
                let plan = join_order_q3::build_q3_logical_plan(&ctx, q, dim_order).await?;

                if cli.explain_physical {
                    print_physical_plan_logical(
                        &ctx,
                        &plan,
                        &format!("SSB {qid} dim_order={order_label}"),
                    )
                    .await?;
                }

                let mut timings = Vec::with_capacity(cli.iterations);
                let mut row_count = 0usize;

                for it in 0..cli.iterations {
                    let flamegraph_svg = (q_idx == 0 && d_idx == 0 && it == 0)
                        .then(|| cli.flamegraph_dir.as_ref().map(|dir| dir.join("flamegraph.svg")))
                        .flatten();

                    let (rows, elapsed) = run_logical_plan_with_optional_flamegraph(
                        &ctx,
                        plan.clone(),
                        flamegraph_svg.as_deref(),
                    )
                    .await?;
                    row_count = rows;
                    timings.push(elapsed);

                    if it == 0 {
                        match expected_rows {
                            None => expected_rows = Some(rows),
                            Some(er) if er != rows => {
                                log::error!(
                                    "Row count mismatch for {qid} order {order_label}: first order had {er}, this had {rows}"
                                );
                            }
                            Some(_) => {}
                        }
                    }
                }

                log::info!(
                    "{:<6} {:<48} {:>10} {:>12.2}",
                    qid,
                    order_label,
                    row_count,
                    mean_wall_ms(&timings)
                );
            }
        } else if let Some(q) = Q4Query::parse(qid) {
            for (d_idx, dim_order) in Q4_DIMENSION_JOIN_PERMUTATIONS.iter().enumerate() {
                let order_label = format_dim_order_q4(dim_order);
                let plan = build_q4_logical_plan(&ctx, q, dim_order).await?;

                if cli.explain_physical {
                    print_physical_plan_logical(
                        &ctx,
                        &plan,
                        &format!("SSB {qid} dim_order={order_label}"),
                    )
                    .await?;
                }

                let mut timings = Vec::with_capacity(cli.iterations);
                let mut row_count = 0usize;

                for it in 0..cli.iterations {
                    let flamegraph_svg = (q_idx == 0 && d_idx == 0 && it == 0)
                        .then(|| cli.flamegraph_dir.as_ref().map(|dir| dir.join("flamegraph.svg")))
                        .flatten();

                    let (rows, elapsed) = run_logical_plan_with_optional_flamegraph(
                        &ctx,
                        plan.clone(),
                        flamegraph_svg.as_deref(),
                    )
                    .await?;
                    row_count = rows;
                    timings.push(elapsed);

                    if it == 0 {
                        match expected_rows {
                            None => expected_rows = Some(rows),
                            Some(er) if er != rows => {
                                log::error!(
                                    "Row count mismatch for {qid} order {order_label}: first order had {er}, this had {rows}"
                                );
                            }
                            Some(_) => {}
                        }
                    }
                }

                log::info!(
                    "{:<6} {:<48} {:>10} {:>12.2}",
                    qid,
                    order_label,
                    row_count,
                    mean_wall_ms(&timings)
                );
            }
        }
    }

    Ok(())
}
