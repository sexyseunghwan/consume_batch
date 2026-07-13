use rust_decimal::Decimal;

use crate::common::*;

/// Per-asset-type amount maps keyed by user_seq, for a single currency/user_seq batch.
pub struct AssetTypeAmounts {
    pub stock: HashMap<i64, Decimal>,
    pub crypto: HashMap<i64, Decimal>,
    pub cash: HashMap<i64, Decimal>,
    pub deposit: HashMap<i64, Decimal>,
    pub saving: HashMap<i64, Decimal>,
}
