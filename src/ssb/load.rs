//! Load SSB `.tbl` files into in-memory [`MemTable`]s (same pattern as TPC-H [`crate::tpch::datagen`]).
//!
//! Expects `ssb-dbgen` output: **comma**-separated fields and a **trailing empty** column per row.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Datelike;
use datafusion::arrow::array::{ArrayRef, Date32Array, Int32Array};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::temporal_conversions::date32_to_datetime;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionContext};

use super::schema::{
    customer_schema, dates_schema, lineorder_csv_schema, lineorder_schema, part_schema,
    supplier_schema,
};

/// Field delimiter in `.tbl` files (`ssb-dbgen` default).
const TBL_DELIMITER: u8 = b',';

fn require_file(dir: &Path, name: &str) -> Result<PathBuf> {
    let p = dir.join(name);
    if p.is_file() {
        Ok(p)
    } else {
        Err(DataFusionError::Execution(format!(
            "SSB data file not found: {} (expected under {})",
            p.display(),
            dir.display()
        )))
    }
}

fn path_to_str(p: &Path) -> Result<&str> {
    p.to_str()
        .ok_or_else(|| DataFusionError::Execution(format!("non-UTF8 path: {}", p.display())))
}

fn csv_opts(schema: &Schema) -> CsvReadOptions<'_> {
    CsvReadOptions::new()
        .has_header(false)
        .delimiter(TBL_DELIMITER)
        .file_extension(".tbl")
        .schema(schema)
}

/// Read one `.tbl` file and collect all batches (full file read into memory).
async fn read_tbl_batches(
    ctx: &SessionContext,
    path: &str,
    schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    let df = ctx.read_csv(path, csv_opts(schema)).await?;
    df.collect().await
}

/// One-time `Date32` → `YYYYMMDD` `Int32`, matching `CAST(to_char(...) AS INT)` from the previous view.
fn date32_column_to_yyyymmdd(arr: &Date32Array) -> Result<Int32Array> {
    let mut vals = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        let days = arr.value(i);
        let ndt = date32_to_datetime(days).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "invalid Arrow Date32 day offset {days} (lo_orderdate / lo_commitdate)"
            ))
        })?;
        vals.push(ndt.year() * 10_000 + ndt.month() as i32 * 100 + ndt.day() as i32);
    }
    Ok(Int32Array::from(vals))
}

/// Rebuild batches with [`lineorder_schema`] (integer date keys) from CSV `Date32` batches.
fn lineorder_batches_with_int_datekeys(
    batches: Vec<RecordBatch>,
    out_schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let schema_ref: &Schema = out_schema.as_ref();
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        out.push(convert_lineorder_batch_dates(&b, schema_ref)?);
    }
    Ok(out)
}

const DATE_COLUMNS: &[&str] = &["lo_orderdate", "lo_commitdate"];

fn convert_lineorder_batch_dates(batch: &RecordBatch, out_schema: &Schema) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    for field in out_schema.fields() {
        let name = field.name();
        let col = batch.column_by_name(name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "lineorder batch missing column `{name}` during date conversion"
            ))
        })?;

        let out_col = if DATE_COLUMNS.contains(&name.as_str()) {
            let d32 = col
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "expected Date32 for `{name}` from CSV, got {:?}",
                        col.data_type()
                    ))
                })?;
            Arc::new(date32_column_to_yyyymmdd(d32)?) as ArrayRef
        } else {
            Arc::clone(col)
        };
        columns.push(out_col);
    }
    Ok(RecordBatch::try_new(Arc::new(out_schema.clone()), columns)?)
}

/// Register a [`MemTable`] like [`crate::tpch::datagen`] (`vec![batches]` = one partition).
fn register_mem(
    ctx: &SessionContext,
    name: &str,
    table_schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        table_schema
    };
    let partitions = vec![batches];
    ctx.register_table(name, Arc::new(MemTable::try_new(schema, partitions)?))?;
    Ok(())
}

/// Load SSB `.tbl` files into memory and register tables on `ctx` (same idea as TPC-H mem tables).
///
/// Expects `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`, and `lineorder.tbl`.
/// The date dimension is registered as **`dates`**. **`lineorder`** stores `lo_orderdate` /
/// `lo_commitdate` as **`Int32`** `YYYYMMDD` keys (converted once at load from Arrow `Date32` CSV
/// columns), so SSB SQL predicates like `lo_orderdate = d_datekey` execute without a view or
/// per-query `to_char` / cast.
pub async fn register_ssb_tables(ctx: &SessionContext, data_dir: &Path) -> Result<()> {
    let customer = require_file(data_dir, "customer.tbl")?;
    let part = require_file(data_dir, "part.tbl")?;
    let supplier = require_file(data_dir, "supplier.tbl")?;
    let date = require_file(data_dir, "date.tbl")?;
    let lineorder = require_file(data_dir, "lineorder.tbl")?;

    let cust_schema = customer_schema();
    let cust_batches = read_tbl_batches(ctx, path_to_str(&customer)?, &cust_schema).await?;
    register_mem(
        ctx,
        "customer",
        Arc::new(cust_schema),
        cust_batches,
    )?;

    let part_schema = part_schema();
    let part_batches = read_tbl_batches(ctx, path_to_str(&part)?, &part_schema).await?;
    register_mem(ctx, "part", Arc::new(part_schema), part_batches)?;

    let sup_schema = supplier_schema();
    let sup_batches = read_tbl_batches(ctx, path_to_str(&supplier)?, &sup_schema).await?;
    register_mem(ctx, "supplier", Arc::new(sup_schema), sup_batches)?;

    let dates_s = dates_schema();
    let date_batches = read_tbl_batches(ctx, path_to_str(&date)?, &dates_s).await?;
    register_mem(ctx, "dates", Arc::new(dates_s), date_batches)?;

    let lo_csv_schema = lineorder_csv_schema();
    let lo_batches_csv = read_tbl_batches(ctx, path_to_str(&lineorder)?, &lo_csv_schema).await?;
    let lo_mem_schema = Arc::new(lineorder_schema());
    let lo_batches = lineorder_batches_with_int_datekeys(lo_batches_csv, Arc::clone(&lo_mem_schema))?;
    register_mem(ctx, "lineorder", lo_mem_schema, lo_batches)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ssb_smoke_q11_when_ssb_data_dir_set() -> Result<()> {
        let dir = match std::env::var("SSB_DATA_DIR") {
            Ok(d) => PathBuf::from(d),
            Err(_) => return Ok(()),
        };
        let ctx = SessionContext::new();
        register_ssb_tables(&ctx, &dir).await?;
        let sql = crate::ssb::queries::get_query("1.1").expect("q1.1");
        ctx.sql(sql).await?.collect().await?;
        Ok(())
    }
}
