use rust_decimal::Decimal;

use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct CryptoPriceHistory {
    pub crypto_seq: i64,
    pub symbol: String,
    pub currency_code: String,
    pub crypto_name: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    pub recorded_at: DateTime<Utc>,
}
