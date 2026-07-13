use crate::common::*;

use crate::dtos::document_with_id::*;

#[doc = "Aggregation Results Structural Data"]
#[derive(Debug, Serialize, Deserialize, Clone, Getters, Setters, new)]
#[getset(get = "pub", set = "pub")]
pub struct AggResultSet<T> {
    pub aggregated_total: f64,
    pub source_list: Vec<DocumentWithId<T>>,
}
