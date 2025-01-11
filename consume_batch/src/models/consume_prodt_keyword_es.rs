use crate::common::*;

#[derive(Serialize, Deserialize, Debug, new)]
pub struct ConsumeProdtKeywordES {
    pub keyword_type: String,
    pub keyword: String,
}
