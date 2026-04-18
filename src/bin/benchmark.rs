//! TPC-H and Star Schema Benchmark (SSB) runner with optional LIP.
//!
//! SSB expects `.tbl` files from `ssb-dbgen` (e.g. build [vadimtk/ssb-dbgen](https://github.com/vadimtk/ssb-dbgen),
//! then `./dbgen -s <SF> -T a` in the output directory). Files: `customer.tbl`, `part.tbl`, `supplier.tbl`,
//! `date.tbl`, `lineorder.tbl`. Comma-separated with a trailing empty field, as from typical `ssb-dbgen`.
//! Files are read once into in-memory tables (like TPC-H), then timed queries run without re-reading disk.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use datafusion::common::{Result, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;

use lip_datafusion::optimizer_rule::LIPOptimizerRule;
use lip_datafusion::ssb::{load, queries as ssb_queries};
use lip_datafusion::tpch::{datagen, queries as tpch_queries};

#[derive(Parser)]
#[command(
    name = "benchmark",
    about = "Run TPC-H or Star Schema Benchmark queries with optional LIP"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate TPC-H data in memory (tpchgen) and run queries Q1–Q22
    Tpch(TpchCmd),
    /// Load SSB `.tbl` files from a directory and run queries 1.1–4.3
    Ssb(SsbCmd),
}

#[derive(Parser)]
struct SharedBenchArgs {
    /// Number of times to repeat each query (mean wall time reported)
    #[arg(long, default_value = "5")]
    iterations: usize,

    /// Enable lookahead information passing (Bloom filters on right-deep hash-join chains)
    #[arg(long, default_value_t = false)]
    lip: bool,

    /// Bloom false-positive rate when `--lip` is set (typical range: 0.001–0.05)
    #[arg(long, default_value_t = 0.01)]
    lip_fp_rate: f32,

    /// Print the optimized physical plan for each query once before timed runs
    #[arg(long, default_value_t = false)]
    explain_physical: bool,
}

#[derive(Parser)]
struct TpchCmd {
    #[command(flatten)]
    common: SharedBenchArgs,

    /// TPC-H scale factor (1.0 ≈ 6 M lineitem rows; use 0.01 for quick tests)
    #[arg(long, default_value = "0.01")]
    scale_factor: f64,

    /// Comma-separated query numbers to run (e.g. "1,3,5,8"). Defaults to all.
    #[arg(long, value_delimiter = ',')]
    queries: Option<Vec<usize>>,
}

#[derive(Parser)]
struct SsbCmd {
    #[command(flatten)]
    common: SharedBenchArgs,

    /// Directory containing `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`, `lineorder.tbl`
    #[arg(long)]
    data_dir: PathBuf,

    /// Comma-separated query ids (e.g. "1.1,2.1,3.4"). Defaults to all 13.
    #[arg(long, value_delimiter = ',')]
    queries: Option<Vec<String>>,
}

fn session_with_lip(lip: bool, lip_fp_rate: f32) -> SessionContext {
    if lip {
        let session_config = SessionConfig::new().set_bool(
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            false,
        );
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(LIPOptimizerRule::new(lip_fp_rate)))
            .build();
        SessionContext::new_with_state(state)
    } else {
        SessionContext::new()
    }
}

fn mean_wall_ms(timings: &[Duration]) -> f64 {
    let n = timings.len() as f64;
    if n == 0.0 {
        return 0.0;
    }
    timings.iter().map(|d| d.as_secs_f64()).sum::<f64>() * 1000.0 / n
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Result<(usize, std::time::Duration)> {
    let df = ctx.sql(sql).await?;
    let start = Instant::now();
    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((row_count, elapsed))
}

/// Build the optimized physical plan and print it (does not execute the query).
async fn print_physical_plan(ctx: &SessionContext, sql: &str, label: &str) -> Result<()> {
    let df = ctx.sql(sql).await?;
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

    match cli.command {
        Commands::Tpch(cmd) => run_tpch(cmd).await,
        Commands::Ssb(cmd) => run_ssb(cmd).await,
    }
}

async fn run_tpch(cmd: TpchCmd) -> Result<()> {
    if cmd.common.iterations == 0 {
        return exec_err!("--iterations must be at least 1");
    }
    let query_nums: Vec<usize> = cmd.queries.unwrap_or_else(|| (1..=22).collect());

    println!("Generating TPC-H data (SF={}) ...", cmd.scale_factor);
    let ctx = session_with_lip(cmd.common.lip, cmd.common.lip_fp_rate);
    datagen::register_tpch_tables(&ctx, cmd.scale_factor).await?;
    println!(
        "Data ready (LIP {}).\n",
        if cmd.common.lip {
            format!("on, fp_rate={}", cmd.common.lip_fp_rate)
        } else {
            "off".to_string()
        }
    );

    println!("{:<8} {:>10} {:>12}", "Query", "Rows", "Avg (ms)");
    println!("{}", "-".repeat(32));

    for &q in &query_nums {
        if q < 1 || q > 22 {
            eprintln!("Skipping invalid query number: {q}");
            continue;
        }
        let sql = match tpch_queries::get_query(q) {
            Some(s) => s,
            None => {
                eprintln!("Q{q:<3} ERROR: unknown query number");
                continue;
            }
        };

        if cmd.common.explain_physical {
            print_physical_plan(&ctx, &sql, &format!("TPC-H Q{q}")).await?;
        }

        let mut timings = Vec::with_capacity(cmd.common.iterations);
        let mut row_count = 0;

        for _ in 0..cmd.common.iterations {
            match run_query(&ctx, &sql).await {
                Ok((rows, elapsed)) => {
                    row_count = rows;
                    timings.push(elapsed);
                }
                Err(e) => {
                    eprintln!("Q{q:<3} ERROR: {e}");
                    timings.clear();
                    break;
                }
            }
        }

        if timings.len() == cmd.common.iterations {
            println!(
                "Q{:<7} {:>10} {:>12.2}",
                q,
                row_count,
                mean_wall_ms(&timings)
            );
        }
    }

    Ok(())
}

async fn run_ssb(cmd: SsbCmd) -> Result<()> {
    if cmd.common.iterations == 0 {
        return exec_err!("--iterations must be at least 1");
    }
    let query_ids: Vec<String> = cmd
        .queries
        .unwrap_or_else(|| ssb_queries::QUERY_IDS.iter().map(|s| (*s).to_string()).collect());

    log::info!(
        "Reading SSB .tbl files from {} into memory ...",
        cmd.data_dir.display()
    );
    let ctx = session_with_lip(cmd.common.lip, cmd.common.lip_fp_rate);
    load::register_ssb_tables(&ctx, &cmd.data_dir).await?;
    log::info!(
        "Data ready (LIP {}).\n",
        if cmd.common.lip {
            format!("on, fp_rate={}", cmd.common.lip_fp_rate)
        } else {
            "off".to_string()
        }
    );

    log::info!("{:<8} {:>10} {:>12}", "Query", "Rows", "Avg (ms)");
    log::info!("{}", "-".repeat(32));

    for qid in &query_ids {
        let sql = match ssb_queries::get_query(qid) {
            Some(s) => s,
            None => {
                log::error!("Q{qid} ERROR: unknown query id (use e.g. 1.1, 2.3)");
                continue;
            }
        };

        if cmd.common.explain_physical {
            print_physical_plan(&ctx, sql, &format!("SSB {qid}")).await?;
        }

        let mut timings = Vec::with_capacity(cmd.common.iterations);
        let mut row_count = 0;

        for _ in 0..cmd.common.iterations {
            match run_query(&ctx, sql).await {
                Ok((rows, elapsed)) => {
                    row_count = rows;
                    timings.push(elapsed);
                }
                Err(e) => {
                    log::error!("Q{qid} ERROR: {e}");
                    timings.clear();
                    break;
                }
            }
        }

        if timings.len() == cmd.common.iterations {
            log::info!(
                "Q{:<7} {:>10} {:>12.2}",
                qid,
                row_count,
                mean_wall_ms(&timings)
            );
        }
    }

    Ok(())
}
