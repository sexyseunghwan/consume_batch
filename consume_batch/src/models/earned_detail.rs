#![allow(dead_code, clippy::too_many_arguments)]
use crate::common::*;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct EarnedDetail {
    pub earned_idx: i64,
    pub earned_name: String,
    pub earned_money: i64,
    pub earned_money_dollor: Decimal,
    pub earned_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub user_seq: i64,
    pub room_seq: i64,
}
