#![allow(clippy::too_many_arguments)]
use crate::common::*;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct SavingAsset {
    pub saving_seq: i64,
    pub saving_name: String,
    pub accum_saving_amount: Decimal,
    pub interest_rate: Decimal,
    pub term_month: i32,
    pub saving_start_date: DateTime<Utc>,
    pub saving_end_date: DateTime<Utc>,
    pub user_seq: i64,
    pub currency_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
