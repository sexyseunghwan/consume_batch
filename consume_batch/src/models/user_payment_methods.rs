#![allow(clippy::too_many_arguments)]
use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct UserPaymentMethods {
    pub payment_method_id: i64,
    pub payment_type_cd: String,
    pub payment_category_cd: String,
    pub card_id: String,
    pub card_alias: String,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: String,
    pub updated_by: Option<String>,
    pub is_default: bool,
    pub user_seq: i64,
    pub card_company_nm: Option<String>,
}
