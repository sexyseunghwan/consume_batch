#![allow(clippy::too_many_arguments)]
use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct UserPaymentMethod {
    pub payment_method_id: i64,
    pub payment_type_cd: String,
    pub payment_category_cd: String,
    pub card_id: String,
    pub card_alias: String,
    pub use_yn: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub user_seq: i64,
}