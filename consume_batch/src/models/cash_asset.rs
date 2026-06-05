use rust_decimal::Decimal;

use crate::common::*;

#[allow(dead_code, clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct CashAsset {
    pub cash_seq: i64,
    pub cash_name: String,
    pub cash: Decimal,
    pub user_seq: i64,
    pub currency_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
