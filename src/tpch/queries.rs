/// Provides the 22 TPC-H queries with default qualification parameters
/// substituted and syntax adjusted for DataFusion compatibility.
///
/// The raw query templates come from `tpchgen::q_and_a::queries`. We apply
/// the standard TPC-H qualification substitution values and fix minor SQL
/// dialect differences (e.g. interval precision, multi-statement Q15).

/// Return the TPC-H query for the given number (1..=22) with qualification
/// parameter values already substituted and DataFusion-compatible syntax.
pub fn get_query(q: usize) -> Option<String> {
    let template = tpchgen::q_and_a::queries::query(q as i32)?;

    let sql = match q {
        1 => substitute(template, &[(":1", "90")])
            .replace(" (3)", ""),
        2 => substitute(template, &[
            (":1", "15"), (":2", "BRASS"), (":3", "EUROPE"),
        ]),
        3 => substitute(template, &[
            (":1", "BUILDING"), (":2", "1995-03-15"),
        ]),
        4 => substitute(template, &[(":1", "1993-07-01")]),
        5 => substitute(template, &[
            (":1", "ASIA"), (":2", "1994-01-01"),
        ]),
        6 => substitute(template, &[
            (":1", "1994-01-01"), (":2", "0.06"), (":3", "24"),
        ]),
        7 => template.to_string(),
        8 => template.to_string(),
        9 => template.to_string(),
        10 => substitute(template, &[(":1", "1993-10-01")]),
        11 => substitute(template, &[
            (":1", "GERMANY"), (":2", "0.0001"),
        ]),
        12 => substitute(template, &[
            (":1", "MAIL"), (":2", "SHIP"), (":3", "1994-01-01"),
        ]),
        13 => substitute(template, &[
            (":1", "special"), (":2", "requests"),
        ]),
        14 => substitute(template, &[(":1", "1995-09-01")]),
        15 => q15_as_cte(),
        16 => substitute(template, &[
            (":10", "9"),
            (":1", "Brand#45"), (":2", "MEDIUM POLISHED"),
            (":3", "49"), (":4", "14"), (":5", "23"),
            (":6", "45"), (":7", "19"), (":8", "3"), (":9", "36"),
        ]),
        17 => substitute(template, &[
            (":1", "Brand#23"), (":2", "MED BOX"),
        ]),
        18 => substitute(template, &[(":1", "300")]),
        19 => substitute(template, &[
            (":1", "Brand#12"), (":2", "Brand#23"), (":3", "Brand#34"),
            (":4", "1"), (":5", "10"), (":6", "20"),
        ]),
        20 => substitute(template, &[
            (":1", "forest"), (":2", "1994-01-01"), (":3", "CANADA"),
        ]),
        21 => substitute(template, &[(":1", "SAUDI ARABIA")]),
        22 => substitute(template, &[
            (":1", "13"), (":2", "31"), (":3", "23"), (":4", "29"),
            (":5", "30"), (":6", "18"), (":7", "17"),
        ]),
        _ => return None,
    };

    Some(sql)
}

fn substitute(template: &str, params: &[(&str, &str)]) -> String {
    let mut sorted: Vec<_> = params.to_vec();
    sorted.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    let mut result = template.to_string();
    for (key, value) in &sorted {
        result = result.replace(key, value);
    }
    result
}

/// Q15 uses CREATE VIEW / SELECT / DROP VIEW which DataFusion can't execute
/// as a single statement.  Rewrite as a CTE instead.
fn q15_as_cte() -> String {
    r#"
with revenue0 (supplier_no, total_revenue) as (
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '1996-01-01'
        and l_shipdate < date '1996-01-01' + interval '3' month
    group by
        l_suppkey
)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey
"#
    .to_string()
}
