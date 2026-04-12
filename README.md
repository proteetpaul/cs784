# LIP on DataFusion

An implementation of **Lookahead Information Passing (LIP)** for robust query
plans, built on top of [Apache DataFusion](https://datafusion.apache.org/).

> Zhu et al., *Looking Ahead Makes Query Plans Robust*, VLDB 2017.

## Prerequisites

* Rust **1.88+** (required by DataFusion 53)

## Building

```bash
cargo build --release
```

## Running the Benchmarks

The `benchmark` binary has two subcommands: **`tpch`** (in-memory data via `tpchgen`) and **`ssb`** (load Star Schema Benchmark `.tbl` files from [ssb-dbgen](https://github.com/vadimtk/ssb-dbgen)).

### TPC-H (`tpch`)

```bash
# Quick smoke-test (scale factor 0.01, all 22 queries)
cargo run --release --bin benchmark -- tpch

# Run specific queries at a larger scale factor
cargo run --release --bin benchmark -- tpch --scale-factor 0.1 --queries 1,3,5,6,8,12

# Repeat each query 3 times and report best-of-3
cargo run --release --bin benchmark -- tpch --scale-factor 0.1 --queries 1,6 --iterations 3
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scale-factor` | `0.01` | TPC-H scale factor (1.0 ≈ 6 M lineitem rows) |
| `--queries` | all | Comma-separated query numbers (1-22) |
| `--iterations` | `1` | Runs per query; best time reported |
| `--lip` | off | Enable LIP optimizer rule |
| `--lip-fp-rate` | `0.01` | Bloom false-positive rate when `--lip` is set |

### Star Schema Benchmark (`ssb`)

Generate tables with `ssb-dbgen` (example: `./dbgen -s 1 -T a`), then point `--data-dir` at the directory containing `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`, and `lineorder.tbl`.

```bash
# All 13 queries (1.1 … 4.3); comma-separated `.tbl` with ISO dates (default)
cargo run --release --bin benchmark -- ssb --data-dir /path/to/ssb-data

# Subset of queries; pipe-delimited files with integer date keys
cargo run --release --bin benchmark -- ssb --data-dir /path/to/ssb-data --format pipe --queries 1.1,2.1,3.4
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | (required) | Directory of `.tbl` files |
| `--format` | `comma` | `comma` (typical dbgen: `YYYY-MM-DD` in lineorder) or `pipe` (integral dates, no trailing field) |
| `--queries` | all | Comma-separated ids: `1.1`, `2.3`, … |
| `--iterations` | `1` | Runs per query; best time reported |
| `--lip` / `--lip-fp-rate` | | Same as TPC-H |

Optional: set `SSB_DATA_DIR` when running `cargo test` to exercise a quick SSB load + Q1.1.

## Project Layout

```
src/
  lib.rs                 Public API
  session.rs             SessionContext setup with TPC-H tables
  bloom_filter.rs        (stub) Bloom filter for LIP
  adaptive_filter.rs     (stub) Adaptive filter reordering (Algorithm 1)
  lip_filter_exec.rs     (stub) Custom ExecutionPlan for LIP filtering
  optimizer_rule.rs      (stub) PhysicalOptimizerRule to inject LIP
  ssb/
    schema.rs            Arrow schemas for SSB `.tbl` columns
    load.rs              Register tables + lineorder date view
    queries.rs           13 standard SSB SQL queries
  tpch/
    schema.rs            Arrow schemas for all 8 TPC-H tables
    datagen.rs           In-memory random data generator
    queries.rs           All 22 TPC-H SQL queries
src/bin/
  benchmark.rs           CLI benchmark runner
```

## Running Tests

```bash
cargo test
```
