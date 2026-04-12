//! Register SSB tables from `ssb-dbgen` `.tbl` files via DataFusion CSV listing.

use std::path::{Path, PathBuf};

use datafusion::common::{DataFusionError, Result};
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

/// Register SSB dimension tables and `lineorder` on `ctx` from `data_dir`.
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

    let base_csv = || {
        CsvReadOptions::new()
            .has_header(false)
            .delimiter(delim)
            .file_extension(".tbl")
    };

    let cust_schema = customer_schema(trail);
    ctx.register_csv(
        "customer",
        path_to_str(&customer)?,
        base_csv().schema(&cust_schema),
    )
    .await?;

    let part_schema = part_schema(trail);
    ctx.register_csv("part", path_to_str(&part)?, base_csv().schema(&part_schema))
        .await?;

    let sup_schema = supplier_schema(trail);
    ctx.register_csv(
        "supplier",
        path_to_str(&supplier)?,
        base_csv().schema(&sup_schema),
    )
    .await?;

    let dates_s = dates_schema(trail);
    ctx.register_csv("dates", path_to_str(&date)?, base_csv().schema(&dates_s))
        .await?;

    if options.lineorder_date_strings {
        let lo_schema = lineorder_raw_schema_string_dates(trail);
        ctx.register_csv(
            "lineorder_raw",
            path_to_str(&lineorder)?,
            base_csv().schema(&lo_schema),
        )
        .await?;

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
        ctx.register_csv(
            "lineorder",
            path_to_str(&lineorder)?,
            base_csv().schema(&lo_schema),
        )
        .await?;
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
