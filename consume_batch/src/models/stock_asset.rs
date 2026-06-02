use rust_decimal::Decimal;

use crate::common::*;

#[allow(dead_code, clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct StockAsset {
    pub stock_asset_seq: i64,
    pub stock_cnt: i64,
    pub avg_purchase_price: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub user_seq: i64,
    pub stock_seq: i64,
}
