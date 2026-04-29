//! Logical plans for SSB queries 3.1–3.3 (fact `lineorder`, three dimensions).
//!
//! Enumerates all `3! = 6` permutations of which dimension joins first. Each binary join
//! places the **dimension scan on the left** and the growing subtree (ending in `lineorder`)
//! on the right, e.g. for order `(d1, d2, d3)` (join `d1` first, then `d2`, then `d3`):
//! `d3 ⋈ (d2 ⋈ (d1 ⋈ lineorder))`.

use std::fmt;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, SortExpr, col, lit};
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

    /// Equijoin predicate (dimension columns on the left subtree, `lineorder` columns on the right).
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

/// All `3!` permutations of the order in which dimensions are attached (outermost-first in the chain).
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

/// Build the full logical plan: each join has **dimension on the left**, subtree on the right;
/// then aggregate and sort (same semantics as the SSB SQL).
///
/// When `skip_aggregate` is true, aggregate, `GROUP BY`, and **`ORDER BY` / sort are all omitted**
/// so the plan ends at the join tree only.
pub async fn build_q3_logical_plan(
    ctx: &SessionContext,
    query: Q3Query,
    dim_order: &[Dimension; 3],
    skip_aggregate: bool,
) -> Result<LogicalPlan> {
    // For permutation (d1, d2, d3): build `d3 ⋈ (d2 ⋈ (d1 ⋈ lineorder))` so the first listed
    // dimension is innermost with the fact, matching the previous "join d1 first" meaning.
    let mut acc = scan_filtered(ctx, "lineorder", None).await?;

    for dim in dim_order {
        let left_dim = scan_filtered(ctx, dim.table_name(), Some(dim.filter_expr(query))).await?;
        acc = LogicalPlanBuilder::from(left_dim)
            .join_on(acc, JoinType::Inner, [dim.join_on_expr()])?
            .build()?;
    }

    if skip_aggregate {
        return LogicalPlanBuilder::from(acc).build();
    }

    let mut plan = LogicalPlanBuilder::from(acc);

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
    let revenue =
        datafusion::functions_aggregate::expr_fn::sum(col("lo_revenue")).alias("revenue");
    plan = plan
        .aggregate(group_cols, vec![revenue])?
        .sort_with_limit(sort_exprs, None)?;

    plan.build()
}
