use crate::common::*;

/// Represents the lag information for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLag {
    /// Partition ID
    pub partition: i32,
    /// Reference group's committed offset for this partition
    pub reference_offset: i64,
    /// Catchup group's committed offset for this partition
    pub catchup_offset: i64,
    /// Calculated lag (reference_offset - catchup_offset), always >= 0
    pub lag: i64,
}

/// Represents the overall lag status for a topic across all partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupLag {
    /// Topic name
    pub topic: String,
    /// Reference consumer group name
    pub reference_group: String,
    /// Catchup consumer group name
    pub catchup_group: String,
    /// Per-partition lag information
    pub partition_lags: Vec<PartitionLag>,
    /// Total lag across all partitions
    pub total_lag: i64,
}
