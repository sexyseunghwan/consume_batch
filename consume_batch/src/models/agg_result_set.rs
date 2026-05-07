use crate::common::*;

use crate::models::document_with_id::*;

#[doc = "Aggregation Results Structural Data"]
#[derive(Debug, Serialize, Deserialize, Clone, Getters, Setters, new)]
#[getset(get = "pub", set = "pub")]
pub struct AggResultSet<T> {
    pub agg_result: f64,
    pub source_list: Vec<DocumentWithId<T>>,
}
