use crate::common::*;

use crate::enums::IndexingType;

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct SpentDetailFromKafka {
    pub spent_idx: i64,
    pub indexing_type: String,
    #[serde(rename = "reg_at")]
    pub registered_at: DateTime<Utc>,
}

impl SpentDetailFromKafka {
    pub fn to_indexing_type(&self) -> anyhow::Result<IndexingType> {
        self.indexing_type
            .parse::<IndexingType>()
            .map_err(|e| anyhow::anyhow!(e))
    }
}
