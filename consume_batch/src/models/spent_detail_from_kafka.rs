use crate::common::*;

use crate::enums::IndexingType;

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct SpentDetailFromKafka {
    pub spent_idx: i64,
    pub indexing_type: String,
    pub reg_at: DateTime<Utc>,
}

impl SpentDetailFromKafka {
    pub fn convert_indexing_type(&self) -> anyhow::Result<IndexingType> {
        self.indexing_type
            .parse::<IndexingType>()
            .map_err(|e| anyhow::anyhow!(e))
    }
}
