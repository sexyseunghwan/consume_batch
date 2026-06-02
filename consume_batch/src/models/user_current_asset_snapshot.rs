use rust_decimal::Decimal;

use crate::common::*;

#[allow(dead_code, clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct UserCurrentAssetSnapshot {
    pub summary_seq: i64,
    pub user_seq: i64,
    pub currency_code: String,
    pub aggregated_at: DateTime<Utc>,
    pub cash_amount: Decimal,
    pub stock_amount: Decimal,
    pub crypto_amount: Decimal,
    pub deposit_amount: Decimal,
    pub saving_amount: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
