use crate::common::*;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult, Getters, new)]
#[getset(get = "pub")]
pub struct SpentDetail {
    pub spent_idx: i64,
    pub spent_name: String,
    pub spent_money: i64,
    pub spent_at: DateTime<Utc>,
    pub should_index: bool,
    pub user_seq: i64,
    pub spent_group_id: i64,
    pub consume_keyword_type_id: i64,
}
