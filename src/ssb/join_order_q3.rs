//! Left-deep logical plans for SSB queries 3.1–3.3 (fact `lineorder`, three dimensions).
//!
//! Enumerates all `3! = 6` permutations of join order: `(((lineorder ⋈ d1) ⋈ d2) ⋈ d3)`.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{col, lit, Expr, JoinType, LogicalPlan, SortExpr};
use datafusion::prelude::SessionContext;

/// SSB query variants supported by this module.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Q3Query {
    Q3_1,
    Q3_2,
    Q3_3,
}

impl Q3Query {
    pub fn parse(id: &str) -> Option<Self> {
        match id {
            "3.1" => Some(Q3Query::Q3_1),
            "3.2" => Some(Q3Query::Q3_2),
            "3.3" => Some(Q3Query::Q3_3),
            _ => None,
        }
    }
}

/// Dimension table (join to `lineorder` on the appropriate key).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dimension {
    Customer,
    Supplier,
    Dates,
}

impl Dimension {
    fn table_name(self) -> &'static str {
        match self {
            Dimension::Customer => "customer",
            Dimension::Supplier => "supplier",
            Dimension::Dates => "dates",
        }
    }

    /// Join predicate between the accumulated plan (which always includes `lineorder`) and this scan.
    fn join_on_expr(self) -> Expr {
        match self {
            Dimension::Customer => col("lo_custkey").eq(col("c_custkey")),
            Dimension::Supplier => col("lo_suppkey").eq(col("s_suppkey")),
            Dimension::Dates => col("lo_orderdate").eq(col("d_datekey")),
        }
    }

    fn filter_expr(self, query: Q3Query) -> Expr {
        match self {
            Dimension::Customer => match query {
                Q3Query::Q3_1 => col("c_region").eq(lit("ASIA")),
                Q3Query::Q3_2 => col("c_nation").eq(lit("UNITED STATES")),
                Q3Query::Q3_3 => col("c_city")
                    .eq(lit("UNITED KI1"))
                    .or(col("c_city").eq(lit("UNITED KI5"))),
            },
            Dimension::Supplier => match query {
                Q3Query::Q3_1 => col("s_region").eq(lit("ASIA")),
                Q3Query::Q3_2 => col("s_nation").eq(lit("UNITED STATES")),
                Q3Query::Q3_3 => col("s_city")
                    .eq(lit("UNITED KI1"))
                    .or(col("s_city").eq(lit("UNITED KI5"))),
            },
            Dimension::Dates => {
                col("d_year")
                    .gt_eq(lit(1992_i32))
                    .and(col("d_year").lt_eq(lit(1997_i32)))
            }
        }
    }
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Dimension::Customer => "customer",
            Dimension::Supplier => "supplier",
            Dimension::Dates => "dates",
        })
    }
}

/// All `3!` permutations of dimension join order after `lineorder`.
pub const DIMENSION_JOIN_PERMUTATIONS: [[Dimension; 3]; 6] = [
    [Dimension::Customer, Dimension::Supplier, Dimension::Dates],
    [Dimension::Customer, Dimension::Dates, Dimension::Supplier],
    [Dimension::Supplier, Dimension::Customer, Dimension::Dates],
    [Dimension::Supplier, Dimension::Dates, Dimension::Customer],
    [Dimension::Dates, Dimension::Customer, Dimension::Supplier],
    [Dimension::Dates, Dimension::Supplier, Dimension::Customer],
];

pub fn format_dim_order(order: &[Dimension; 3]) -> String {
    format!("{}→{}→{}", order[0], order[1], order[2])
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
pub async fn build_q3_logical_plan(
    ctx: &SessionContext,
    query: Q3Query,
    dim_order: &[Dimension; 3],
) -> Result<LogicalPlan> {
    let lo_src = Arc::new(DefaultTableSource::new(
        ctx.table_provider("lineorder").await?,
    ));
    let mut plan = LogicalPlanBuilder::scan("lineorder", lo_src, None)?;

    for dim in dim_order {
        let right = scan_filtered(ctx, dim.table_name(), Some(dim.filter_expr(query))).await?;
        plan = plan.join_on(right, JoinType::Inner, [dim.join_on_expr()])?;
    }

    let (group_cols, sort_exprs): (Vec<Expr>, Vec<SortExpr>) = match query {
        Q3Query::Q3_1 => (
            vec![col("c_nation"), col("s_nation"), col("d_year")],
            vec![
                SortExpr::new(col("d_year"), true, false),
                SortExpr::new(col("revenue"), false, false),
            ],
        ),
        Q3Query::Q3_2 | Q3Query::Q3_3 => (
            vec![col("c_city"), col("s_city"), col("d_year")],
            vec![
                SortExpr::new(col("d_year"), true, false),
                SortExpr::new(col("revenue"), false, false),
            ],
        ),
    };

    let revenue = datafusion::functions_aggregate::expr_fn::sum(col("lo_revenue")).alias("revenue");
    plan = plan
        .aggregate(group_cols, vec![revenue])?
        .sort_with_limit(sort_exprs, None)?;

    plan.build()
}
