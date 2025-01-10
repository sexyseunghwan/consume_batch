use crate::common::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsumeProdtKeywordES {
    pub keyword_type: String,
    pub keyword: String,
    pub bias_value: i32,
}
