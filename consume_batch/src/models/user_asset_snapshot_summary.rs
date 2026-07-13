use rust_decimal::Decimal;
use sea_orm::ActiveValue;

use crate::common::*;
use crate::entity::user_asset_snapshot_summary as entity;

#[allow(dead_code, clippy::too_many_arguments)]
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct UserAssetSnapshotSummary {
    pub summary_seq: i64,
    pub user_seq: i64,
    pub aggregated_at: DateTime<Utc>,
    pub total_asset_amount: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
}

impl From<UserAssetSnapshotSummary> for entity::ActiveModel {
    fn from(src: UserAssetSnapshotSummary) -> Self {
        Self {
            summary_seq: ActiveValue::NotSet,
            user_seq: ActiveValue::Set(src.user_seq),
            aggregated_at: ActiveValue::Set(src.aggregated_at.naive_utc()),
            total_asset_amount: ActiveValue::Set(src.total_asset_amount),
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
