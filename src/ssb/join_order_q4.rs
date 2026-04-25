//! Left-deep logical plans for SSB queries 4.1–4.3 (fact `lineorder`, four dimensions).
//!
//! Enumerates all `4! = 24` permutations of join order: `((((lineorder ⋈ d1) ⋈ d2) ⋈ d3) ⋈ d4)`.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{col, lit, Expr, JoinType, LogicalPlan, SortExpr};
use datafusion::prelude::SessionContext;

/// SSB query variants supported by this module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Q4Query {
    Q4_1,
    Q4_2,
    Q4_3,
}

impl Q4Query {
    pub fn parse(id: &str) -> Option<Self> {
        match id {
            "4.1" => Some(Q4Query::Q4_1),
            "4.2" => Some(Q4Query::Q4_2),
            "4.3" => Some(Q4Query::Q4_3),
            _ => None,
        }
    }
}

/// Dimension table (join to `lineorder` on the appropriate key).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dimension {
    Customer,
    Supplier,
    Part,
    Dates,
}

impl Dimension {
    fn table_name(self) -> &'static str {
        match self {
            Dimension::Customer => "customer",
            Dimension::Supplier => "supplier",
            Dimension::Part => "part",
            Dimension::Dates => "dates",
        }
    }

    /// Join predicate between the accumulated plan (which always includes `lineorder`) and this scan.
    fn join_on_expr(self) -> Expr {
        match self {
            Dimension::Customer => col("lo_custkey").eq(col("c_custkey")),
            Dimension::Supplier => col("lo_suppkey").eq(col("s_suppkey")),
            Dimension::Part => col("lo_partkey").eq(col("p_partkey")),
            Dimension::Dates => col("lo_orderdate").eq(col("d_datekey")),
        }
    }

    /// Predicate pushed into the dimension scan; `None` means no filter on that scan.
    fn filter_expr(self, query: Q4Query) -> Option<Expr> {
        match self {
            Dimension::Customer => match query {
                Q4Query::Q4_1 | Q4Query::Q4_2 => Some(col("c_region").eq(lit("AMERICA"))),
                Q4Query::Q4_3 => None,
            },
            Dimension::Supplier => match query {
                Q4Query::Q4_1 | Q4Query::Q4_2 => Some(col("s_region").eq(lit("AMERICA"))),
                Q4Query::Q4_3 => Some(col("s_nation").eq(lit("UNITED STATES"))),
            },
            Dimension::Part => match query {
                Q4Query::Q4_1 | Q4Query::Q4_2 => Some(
                    col("p_mfgr")
                        .eq(lit("MFGR#1"))
                        .or(col("p_mfgr").eq(lit("MFGR#2"))),
                ),
                Q4Query::Q4_3 => Some(col("p_category").eq(lit("MFGR#14"))),
            },
            Dimension::Dates => match query {
                Q4Query::Q4_1 => None,
                Q4Query::Q4_2 | Q4Query::Q4_3 => Some(
                    col("d_year")
                        .eq(lit(1997_i32))
                        .or(col("d_year").eq(lit(1998_i32))),
                ),
            },
        }
    }
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Dimension::Customer => "customer",
            Dimension::Supplier => "supplier",
            Dimension::Part => "part",
            Dimension::Dates => "dates",
        })
    }
}

/// All `4!` permutations of dimension join order after `lineorder`.
pub const Q4_DIMENSION_JOIN_PERMUTATIONS: [[Dimension; 4]; 24] = [
    [
        Dimension::Customer,
        Dimension::Supplier,
        Dimension::Part,
        Dimension::Dates,
    ],
    [
        Dimension::Customer,
        Dimension::Supplier,
        Dimension::Dates,
        Dimension::Part,
    ],
    [
        Dimension::Customer,
        Dimension::Part,
        Dimension::Supplier,
        Dimension::Dates,
    ],
    [
        Dimension::Customer,
        Dimension::Part,
        Dimension::Dates,
        Dimension::Supplier,
    ],
    [
        Dimension::Customer,
        Dimension::Dates,
        Dimension::Supplier,
        Dimension::Part,
    ],
    [
        Dimension::Customer,
        Dimension::Dates,
        Dimension::Part,
        Dimension::Supplier,
    ],
    [
        Dimension::Supplier,
        Dimension::Customer,
        Dimension::Part,
        Dimension::Dates,
    ],
    [
        Dimension::Supplier,
        Dimension::Customer,
        Dimension::Dates,
        Dimension::Part,
    ],
    [
        Dimension::Supplier,
        Dimension::Part,
        Dimension::Customer,
        Dimension::Dates,
    ],
    [
        Dimension::Supplier,
        Dimension::Part,
        Dimension::Dates,
        Dimension::Customer,
    ],
    [
        Dimension::Supplier,
        Dimension::Dates,
        Dimension::Customer,
        Dimension::Part,
    ],
    [
        Dimension::Supplier,
        Dimension::Dates,
        Dimension::Part,
        Dimension::Customer,
    ],
    [
        Dimension::Part,
        Dimension::Customer,
        Dimension::Supplier,
        Dimension::Dates,
    ],
    [
        Dimension::Part,
        Dimension::Customer,
        Dimension::Dates,
        Dimension::Supplier,
    ],
    [
        Dimension::Part,
        Dimension::Supplier,
        Dimension::Customer,
        Dimension::Dates,
    ],
    [
        Dimension::Part,
        Dimension::Supplier,
        Dimension::Dates,
        Dimension::Customer,
    ],
    [
        Dimension::Part,
        Dimension::Dates,
        Dimension::Customer,
        Dimension::Supplier,
    ],
    [
        Dimension::Part,
        Dimension::Dates,
        Dimension::Supplier,
        Dimension::Customer,
    ],
    [
        Dimension::Dates,
        Dimension::Customer,
        Dimension::Supplier,
        Dimension::Part,
    ],
    [
        Dimension::Dates,
        Dimension::Customer,
        Dimension::Part,
        Dimension::Supplier,
    ],
    [
        Dimension::Dates,
        Dimension::Supplier,
        Dimension::Customer,
        Dimension::Part,
    ],
    [
        Dimension::Dates,
        Dimension::Supplier,
        Dimension::Part,
        Dimension::Customer,
    ],
    [
        Dimension::Dates,
        Dimension::Part,
        Dimension::Customer,
        Dimension::Supplier,
    ],
    [
        Dimension::Dates,
        Dimension::Part,
        Dimension::Supplier,
        Dimension::Customer,
    ],
];

pub fn format_dim_order(order: &[Dimension; 4]) -> String {
    format!(
        "{}→{}→{}→{}",
        order[0], order[1], order[2], order[3]
    )
}

async fn scan_filtered(
    ctx: &SessionContext,
    table: &str,
    filter: Option<Expr>,
) -> Result<LogicalPlan> {
    let provider = ctx.table_provider(table).await?;
    let source = Arc::new(DefaultTableSource::new(provider));
    let mut b = LogicalPlanBuilder::scan(table, source, None)?;
    if let Some(f) = filter {
        b = b.filter(f)?;
    }
    b.build()
}

/// Build the full logical plan: left-deep joins, aggregate, sort (matches SSB SQL semantics).
pub async fn build_q4_logical_plan(
    ctx: &SessionContext,
    query: Q4Query,
    dim_order: &[Dimension; 4],
) -> Result<LogicalPlan> {
    let lo_src = Arc::new(DefaultTableSource::new(
        ctx.table_provider("lineorder").await?,
    ));
    let mut plan = LogicalPlanBuilder::scan("lineorder", lo_src, None)?;

    for dim in dim_order {
        let right = scan_filtered(ctx, dim.table_name(), dim.filter_expr(query)).await?;
        plan = plan.join_on(right, JoinType::Inner, [dim.join_on_expr()])?;
    }

    let profit = datafusion::functions_aggregate::expr_fn::sum(
        col("lo_revenue") - col("lo_supplycost"),
    )
    .alias("profit");

    let (group_cols, sort_exprs): (Vec<Expr>, Vec<SortExpr>) = match query {
        Q4Query::Q4_1 => (
            vec![col("d_year"), col("c_nation")],
            vec![
                SortExpr::new(col("d_year"), true, false),
                SortExpr::new(col("c_nation"), true, false),
            ],
        ),
        Q4Query::Q4_2 => (
            vec![col("d_year"), col("s_nation"), col("p_category")],
            vec![
                SortExpr::new(col("d_year"), true, false),
                SortExpr::new(col("s_nation"), true, false),
                SortExpr::new(col("p_category"), true, false),
            ],
        ),
        Q4Query::Q4_3 => (
            vec![col("d_year"), col("s_city"), col("p_brand")],
            vec![
                SortExpr::new(col("d_year"), true, false),
                SortExpr::new(col("s_city"), true, false),
                SortExpr::new(col("p_brand"), true, false),
            ],
        ),
    };

    plan = plan
        .aggregate(group_cols, vec![profit])?
        .sort_with_limit(sort_exprs, None)?;

    plan.build()
}
