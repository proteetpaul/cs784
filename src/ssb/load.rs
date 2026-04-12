//! Load SSB `.tbl` files into in-memory [`MemTable`]s (same pattern as TPC-H [`crate::tpch::datagen`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionContext};

use super::schema::{
    customer_schema, dates_schema, lineorder_raw_schema_integral_dates,
    lineorder_raw_schema_string_dates, part_schema, supplier_schema,
};

/// How to parse `.tbl` files (comma + string dates is typical for vadimtk/ssb-dbgen).
#[derive(Clone, Copy, Debug)]
pub struct SsbLoadOptions {
    pub delimiter: u8,
    /// When true, `lo_orderdate` / `lo_commitdate` are read as `YYYY-MM-DD` strings and exposed
    /// as `YYYYMMDD` integers through view `lineorder` so `lo_orderdate = d_datekey` works.
    pub lineorder_date_strings: bool,
    /// Many dbgen builds emit a trailing empty field (extra `|` or `,`); model it as nullable `_trail`.
    pub trailing_empty_field: bool,
}

impl Default for SsbLoadOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            lineorder_date_strings: true,
            trailing_empty_field: true,
        }
    }
}

impl SsbLoadOptions {
    /// Comma-separated rows, ISO date strings in lineorder (common open-source `dbgen -T a` output).
    pub fn comma_dbgen() -> Self {
        Self::default()
    }

    /// Pipe-separated rows with integer date keys in lineorder (no view, no trailing column).
    pub fn pipe_integral_dates() -> Self {
        Self {
            delimiter: b'|',
            lineorder_date_strings: false,
            trailing_empty_field: false,
        }
    }
}

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

fn csv_opts(delimiter: u8, schema: &Schema) -> CsvReadOptions<'_> {
    CsvReadOptions::new()
        .has_header(false)
        .delimiter(delimiter)
        .file_extension(".tbl")
        .schema(schema)
}

/// Read one `.tbl` file and collect all batches (full file read into memory).
async fn read_tbl_batches(
    ctx: &SessionContext,
    path: &str,
    delimiter: u8,
    schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    let df = ctx.read_csv(path, csv_opts(delimiter, schema)).await?;
    df.collect().await
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
/// The date dimension is registered as **`dates`** to match standard SSB query SQL.
pub async fn register_ssb_tables(
    ctx: &SessionContext,
    data_dir: &Path,
    options: SsbLoadOptions,
) -> Result<()> {
    let trail = options.trailing_empty_field;
    let delim = options.delimiter;

    let customer = require_file(data_dir, "customer.tbl")?;
    let part = require_file(data_dir, "part.tbl")?;
    let supplier = require_file(data_dir, "supplier.tbl")?;
    let date = require_file(data_dir, "date.tbl")?;
    let lineorder = require_file(data_dir, "lineorder.tbl")?;

    let cust_schema = customer_schema(trail);
    let cust_batches = read_tbl_batches(ctx, path_to_str(&customer)?, delim, &cust_schema).await?;
    register_mem(
        ctx,
        "customer",
        Arc::new(cust_schema),
        cust_batches,
    )?;

    let part_schema = part_schema(trail);
    let part_batches = read_tbl_batches(ctx, path_to_str(&part)?, delim, &part_schema).await?;
    register_mem(ctx, "part", Arc::new(part_schema), part_batches)?;

    let sup_schema = supplier_schema(trail);
    let sup_batches = read_tbl_batches(ctx, path_to_str(&supplier)?, delim, &sup_schema).await?;
    register_mem(ctx, "supplier", Arc::new(sup_schema), sup_batches)?;

    let dates_s = dates_schema(trail);
    let date_batches = read_tbl_batches(ctx, path_to_str(&date)?, delim, &dates_s).await?;
    register_mem(ctx, "dates", Arc::new(dates_s), date_batches)?;

    if options.lineorder_date_strings {
        let lo_schema = lineorder_raw_schema_string_dates(trail);
        let lo_batches =
            read_tbl_batches(ctx, path_to_str(&lineorder)?, delim, &lo_schema).await?;
        register_mem(
            ctx,
            "lineorder_raw",
            Arc::new(lo_schema),
            lo_batches,
        )?;

        let view_sql = r#"
CREATE OR REPLACE VIEW lineorder AS
SELECT
    lo_orderkey,
    lo_linenumber,
    lo_custkey,
    lo_partkey,
    lo_suppkey,
    CAST(REPLACE(lo_orderdate, '-', '') AS INT) AS lo_orderdate,
    lo_orderpriority,
    lo_shippriority,
    lo_quantity,
    lo_extendedprice,
    lo_ordtotalprice,
    lo_discount,
    lo_revenue,
    lo_supplycost,
    lo_tax,
    CAST(REPLACE(lo_commitdate, '-', '') AS INT) AS lo_commitdate,
    lo_shipmode
FROM lineorder_raw
"#;
        ctx.sql(view_sql).await?.collect().await?;
    } else {
        let lo_schema = lineorder_raw_schema_integral_dates(trail);
        let lo_batches =
            read_tbl_batches(ctx, path_to_str(&lineorder)?, delim, &lo_schema).await?;
        register_mem(ctx, "lineorder", Arc::new(lo_schema), lo_batches)?;
    }

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
        register_ssb_tables(&ctx, &dir, SsbLoadOptions::default()).await?;
        let sql = crate::ssb::queries::get_query("1.1").expect("q1.1");
        ctx.sql(sql).await?.collect().await?;
        Ok(())
    }
}
