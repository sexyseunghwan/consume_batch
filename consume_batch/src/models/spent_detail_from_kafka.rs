use crate::common::*;

use crate::enums::IndexingType;

#[derive(Debug, Clone, Serialize, Deserialize, new)]
pub struct SpentDetailFromKafka {
    pub spent_idx: i64,
    pub indexing_type: String,
    pub reg_at: DateTime<Utc>,
}

impl SpentDetailFromKafka {
    /// Converts the Kafka indexing type string into an `IndexingType`.
    ///
    /// Parses the `indexing_type` field carried by a Kafka message into the enum
    /// used by the indexing pipeline.
    ///
    /// # Returns
    ///
    /// Returns the parsed `IndexingType`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `indexing_type` does not match a supported indexing event type
    pub fn to_indexing_type(&self) -> anyhow::Result<IndexingType> {
        self.indexing_type
            .parse::<IndexingType>()
            .map_err(|e| anyhow::anyhow!(e))
    }
}
