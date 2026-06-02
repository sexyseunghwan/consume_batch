#![allow(dead_code, clippy::too_many_arguments)]
use crate::common::*;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct DepositAsset {
    pub deposit_seq: i64,
    pub deposit_name: String,
    pub deposit_amount: Decimal,
    pub interest_rate: Decimal,
    pub deposit_start_date: DateTime<Utc>,
    pub deposit_end_date: DateTime<Utc>,
    pub user_seq: i64,
    pub currency_code: String,
    pub is_terminated: bool,
    pub term_month: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
