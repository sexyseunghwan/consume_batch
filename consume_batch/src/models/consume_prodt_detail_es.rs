use crate::common::*;


#[derive(Debug, Getters, Serialize, Deserialize, new)]
#[getset(get = "pub")]
pub struct ConsumeProdtDetailES {
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub cur_timestamp: Option<String>,
    pub prodt_name: String,
    pub prodt_money: i32
}