//! Arrow schemas for SSB `.tbl` files.
//!
//! Layout matches `ssb-dbgen` output (e.g. [vadimtk/ssb-dbgen](https://github.com/vadimtk/ssb-dbgen)):
//! comma-separated fields, optional double quotes, and a trailing empty field per row.
//! `lo_orderdate` / `lo_commitdate` are read as Arrow `Date32` from `YYYY-MM-DD` or `YYYYMMDD`
//! text in the file; `load` exposes integer `d_datekey`-compatible keys via view `lineorder`.

use datafusion::arrow::datatypes::{DataType, Field, Schema};

fn trail(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

/// `customer.tbl`: 8 data columns + trailing empty field (`_trail`).
pub fn customer_schema() -> Schema {
    Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_city", DataType::Utf8, false),
        Field::new("c_nation", DataType::Utf8, false),
        Field::new("c_region", DataType::Utf8, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        trail("_trail"),
    ])
}

/// `part.tbl`
pub fn part_schema() -> Schema {
    Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_category", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_color", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        trail("_trail"),
    ])
}

/// `supplier.tbl`
pub fn supplier_schema() -> Schema {
    Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_city", DataType::Utf8, false),
        Field::new("s_nation", DataType::Utf8, false),
        Field::new("s_region", DataType::Utf8, false),
        Field::new("s_phone", DataType::Utf8, false),
        trail("_trail"),
    ])
}

/// `date.tbl` — registered as SQL table `dates`.
pub fn dates_schema() -> Schema {
    Schema::new(vec![
        Field::new("d_datekey", DataType::Int32, false),
        Field::new("d_date", DataType::Utf8, false),
        Field::new("d_dayofweek", DataType::Utf8, false),
        Field::new("d_month", DataType::Utf8, false),
        Field::new("d_year", DataType::Int32, false),
        Field::new("d_yearmonthnum", DataType::Int32, false),
        Field::new("d_yearmonth", DataType::Utf8, false),
        Field::new("d_daynuminweek", DataType::Int32, false),
        Field::new("d_daynuminmonth", DataType::Int32, false),
        Field::new("d_daynuminyear", DataType::Int32, false),
        Field::new("d_monthnuminyear", DataType::Int32, false),
        Field::new("d_weeknuminyear", DataType::Int32, false),
        Field::new("d_sellingseason", DataType::Utf8, false),
        Field::new("d_lastdayinweekfl", DataType::Utf8, false),
        Field::new("d_lastdayinmonthfl", DataType::Utf8, false),
        Field::new("d_holidayfl", DataType::Utf8, false),
        Field::new("d_weekdayfl", DataType::Utf8, false),
        trail("_trail"),
    ])
}

/// `lineorder.tbl` — `lo_orderdate` / `lo_commitdate` parsed as `Date32` from `YYYY-MM-DD` or compact `YYYYMMDD` in the file.
pub fn lineorder_schema() -> Schema {
    Schema::new(vec![
        Field::new("lo_orderkey", DataType::Int64, false),
        Field::new("lo_linenumber", DataType::Int32, false),
        Field::new("lo_custkey", DataType::Int32, false),
        Field::new("lo_partkey", DataType::Int32, false),
        Field::new("lo_suppkey", DataType::Int32, false),
        Field::new("lo_orderdate", DataType::Date32, false),
        Field::new("lo_orderpriority", DataType::Utf8, false),
        Field::new("lo_shippriority", DataType::Int32, false),
        Field::new("lo_quantity", DataType::Int32, false),
        Field::new("lo_extendedprice", DataType::Int64, false),
        Field::new("lo_ordtotalprice", DataType::Int64, false),
        Field::new("lo_discount", DataType::Int32, false),
        Field::new("lo_revenue", DataType::Int64, false),
        Field::new("lo_supplycost", DataType::Int64, false),
        Field::new("lo_tax", DataType::Int32, false),
        Field::new("lo_commitdate", DataType::Date32, false),
        Field::new("lo_shipmode", DataType::Utf8, false),
        trail("_trail"),
    ])
}
