use crate::common::*;

#[derive(Debug, Getters, Serialize, Deserialize, Clone, new)]
#[getset(get = "pub")]
pub struct ConsumingIndexProdtType {
    pub consume_keyword_type_id: i64,
    pub consume_keyword_type: String,
    pub consume_keyword: String,
    pub keyword_weight: i32,
}
