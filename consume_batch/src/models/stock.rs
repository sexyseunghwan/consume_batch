use rust_decimal::Decimal;

use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct Stock {
    pub stock_seq: i64,
    pub market_seq: i64,
    pub stock_name: String,
    pub api_symbol: String,
    pub stock_price: Option<Decimal>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}
