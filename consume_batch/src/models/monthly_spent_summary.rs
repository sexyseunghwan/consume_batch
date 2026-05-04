use crate::common::*;
use sea_orm::FromQueryResult;

/// Per-user, per-category monthly spend aggregate.
///
/// Produced by a raw GROUP BY query on `SPENT_DETAIL_INDEXING`
/// for a given year/month. One row per (user, category) pair.
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters)]
#[getset(get = "pub")]
pub struct UserMonthlySpentSummary {
    pub user_seq: i64,
    /// Login ID — used as the recipient email address.
    pub user_id: String,
    pub consume_keyword_type: String,
    pub total_money: i64,
}
