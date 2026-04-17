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

# Repeat each query 10 times and report mean time (default is 5 iterations)
cargo run --release --bin benchmark -- tpch --scale-factor 0.1 --queries 1,6 --iterations 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scale-factor` | `0.01` | TPC-H scale factor (1.0 ≈ 6 M lineitem rows) |
| `--queries` | all | Comma-separated query numbers (1-22) |
| `--iterations` | `5` | Runs per query; mean wall time reported |
| `--explain-physical` | off | Print optimized physical plan per query before timed runs |
| `--lip` | off | Enable LIP optimizer rule |
| `--lip-fp-rate` | `0.01` | Bloom false-positive rate when `--lip` is set |

### Star Schema Benchmark (`ssb`)

Generate tables with `ssb-dbgen` (example: `./dbgen -s 1 -T a`), then point `--data-dir` at the directory containing `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`, and `lineorder.tbl`. The loader expects **comma-separated** rows with a **trailing empty field**, as produced by typical dbgen output. Files are read once into DataFusion `MemTable`s (same pattern as in-memory TPC-H), then queries run without further disk I/O for those tables.

```bash
# All 13 queries (1.1 … 4.3)
cargo run --release --bin benchmark -- ssb --data-dir /path/to/ssb-data

# Subset of queries
cargo run --release --bin benchmark -- ssb --data-dir /path/to/ssb-data --queries 1.1,2.1,3.4
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | (required) | Directory of comma-separated `.tbl` files (trailing empty column) |
| `--queries` | all | Comma-separated ids: `1.1`, `2.3`, … |
| `--iterations` | `5` | Runs per query; mean wall time reported |
| `--explain-physical` | off | Same as TPC-H |
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
    load.rs              Read `.tbl` into MemTable + lineorder date view
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
