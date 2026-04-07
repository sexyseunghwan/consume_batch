#![allow(clippy::too_many_arguments)]
use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct SpentDetailIndexing {
    pub spent_idx: i64,
    pub spent_name: String,
    pub spent_money: i32,
    pub spent_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub user_seq: i64,
    pub consume_keyword_type_id: i64,
    pub consume_keyword_type: String,
    pub room_seq: i64,
    pub user_id: String,
    pub card_alias: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<String>,
}