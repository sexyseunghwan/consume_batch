use rust_decimal::Decimal;

use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct AssetAmount {
    pub user_seq: i64,
    pub asset_sum: Option<Decimal>,
}
