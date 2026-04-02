#![allow(dead_code)]
use crate::common::*;
use crate::models::ConsumerGroupLag;

/// Trait defining consume service operations.
///
/// This trait provides an abstraction layer for Kafka message consumption,
/// used by batch processing jobs to retrieve messages from Kafka topics.
#[async_trait]
pub trait ConsumeService: Send + Sync {
    /// Consumes messages from a Kafka topic with a specified limit.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to consume
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<Value>)` containing the consumed messages as JSON values.
    async fn consume_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<Value>, anyhow::Error>;

    /// Consumes a single message from a Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(Value))` if a message is available, `Ok(None)` if no message.
    async fn consume_one(&self, topic: &str) -> Result<Option<Value>, anyhow::Error>;

    /// Consumes messages from a Kafka topic and deserializes them into `T`.
    async fn consume_messages_as<T>(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        T: DeserializeOwned;

    /// Consumes messages from a Kafka topic with a custom consumer group.
    ///
    /// Allows multiple independent consumers for the same topic with different offsets.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to consume
    /// * `group_suffix` - Group ID suffix (e.g., "full-index", "incremental")
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// // Two independent consumers for the same topic
    /// let full_msgs = service.consume_messages_with_group("orders", 100, "full-index").await?;
    /// let incr_msgs = service.consume_messages_with_group("orders", 100, "incremental").await?;
    /// ```
    async fn consume_messages_with_group(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> Result<Vec<Value>, anyhow::Error>;

    /// Consumes and deserializes messages with a custom consumer group.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Target type for deserialization (must implement `DeserializeOwned`)
    async fn consume_messages_as_with_group<T>(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        T: DeserializeOwned;

    /// Copies committed offsets from one consumer group to another.
    async fn replicate_consumer_group_offsets(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> anyhow::Result<()>;

    /// Returns how many messages `catchup_group` is behind `reference_group` on the given topic.
    ///
    /// lag = sum(reference_group committed offsets) - sum(catchup_group committed offsets)
    ///
    /// Returns 0 if `catchup_group` has reached or exceeded `reference_group`.
    ///
    /// # Deprecated
    ///
    /// This method sums all partition offsets and compares totals, which can be misleading.
    /// Use `get_consumer_group_lag_by_partition` instead for accurate per-partition lag tracking.
    async fn get_consumer_group_lag(
        &self,
        topic: &str,
        reference_group: &str,
        catchup_group: &str,
    ) -> anyhow::Result<i64>;

    /// Returns detailed lag information per partition for a consumer group.
    ///
    /// Compares committed offsets between `reference_group` and `catchup_group`
    /// on a per-partition basis, providing accurate lag tracking for each partition.
    ///
    /// # Arguments
    ///
    /// * `topic`           - The Kafka topic to analyze
    /// * `reference_group` - The consumer group to use as reference (usually the primary consumer)
    /// * `catchup_group`   - The consumer group to measure lag for (usually a replica or backup)
    ///
    /// # Returns
    ///
    /// Returns a `ConsumerGroupLag` struct containing:
    /// - Per-partition lag information (partition ID, offsets, and lag)
    /// - Total lag across all partitions
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// let lag_info = service
    ///     .get_consumer_group_lag_by_partition("orders_topic", "primary-group", "backup-group")
    ///     .await?;
    ///
    /// for partition_lag in &lag_info.partition_lags {
    ///     println!("Partition {}: lag = {}", partition_lag.partition, partition_lag.lag);
    /// }
    /// println!("Total lag: {}", lag_info.total_lag);
    /// ```
    async fn get_consumer_group_lag_by_partition(
        &self,
        topic: &str,
        reference_group: &str,
        catchup_group: &str,
    ) -> anyhow::Result<ConsumerGroupLag>;
}
