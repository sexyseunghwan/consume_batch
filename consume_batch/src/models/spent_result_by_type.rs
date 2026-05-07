use crate::common::*;

#[derive(Debug, Getters, Serialize, Deserialize, new)]
#[getset(get = "pub")]
pub struct SpentResultByType {
    pub spent_type: String,
    pub spent_cost: i64,
    pub spent_per: f64,
}
