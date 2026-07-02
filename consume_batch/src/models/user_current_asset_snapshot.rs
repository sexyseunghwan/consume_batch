use rust_decimal::Decimal;
use sea_orm::ActiveValue;

use crate::common::*;
use crate::entity::user_current_asset_snapshot as entity;

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

impl From<UserCurrentAssetSnapshot> for entity::ActiveModel {
    fn from(src: UserCurrentAssetSnapshot) -> Self {
        Self {
            summary_seq: ActiveValue::NotSet,
            user_seq: ActiveValue::Set(src.user_seq),
            currency_code: ActiveValue::Set(src.currency_code),
            aggregated_at: ActiveValue::Set(src.aggregated_at.naive_utc()),
            cash_amount: ActiveValue::Set(src.cash_amount),
            stock_amount: ActiveValue::Set(src.stock_amount),
            crypto_amount: ActiveValue::Set(src.crypto_amount),
            deposit_amount: ActiveValue::Set(src.deposit_amount),
            saving_amount: ActiveValue::Set(src.saving_amount),
            created_at: ActiveValue::Set(src.created_at.naive_utc()),
            updated_at: match src.updated_at {
                Some(updated_at) => ActiveValue::Set(Some(updated_at.naive_utc())),
                None => ActiveValue::NotSet,
            },
            created_by: ActiveValue::Set(src.created_by),
            updated_by: match src.updated_by {
                Some(updated_by) => ActiveValue::Set(Some(updated_by)),
                None => ActiveValue::NotSet,
            },
        }
    }
}
