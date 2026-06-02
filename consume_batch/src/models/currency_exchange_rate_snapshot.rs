use rust_decimal::Decimal;

use crate::common::*;

#[allow(clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct CurrencyExchangeRateSnapshot {
    pub exchange_rate_snapshot_seq: i64,
    pub base_currency_code: String,
    pub target_currency_code: String,
    pub base_amount: Decimal,
    pub exchange_rate: Decimal,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
