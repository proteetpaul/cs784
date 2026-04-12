use datafusion::common::Result;
use datafusion::prelude::SessionContext;

use crate::tpch::datagen;

/// Build a [`SessionContext`] with all TPC-H tables registered in memory.
///
/// `scale_factor` controls the data volume (1.0 ≈ 6 M lineitem rows).
/// Use small values like 0.01 for quick smoke-tests.
pub async fn create_session(scale_factor: f64) -> Result<SessionContext> {
    let ctx = SessionContext::new();
    datagen::register_tpch_tables(&ctx, scale_factor).await?;
    Ok(ctx)
}
