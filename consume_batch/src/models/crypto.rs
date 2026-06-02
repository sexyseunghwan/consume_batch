use rust_decimal::Decimal;

use crate::common::*;

#[allow(clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct Crypto {
    pub crypto_seq: i64,
    pub crypto_name: String,
    pub crypto_price: Decimal,
    pub api_symbol: String,
    pub currency_code: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
