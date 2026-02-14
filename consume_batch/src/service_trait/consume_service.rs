use crate::common::*;

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
}
