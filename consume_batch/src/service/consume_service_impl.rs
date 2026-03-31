//! Consume service implementation.
//!
//! This module provides the service layer for Kafka message consumption,
//! acting as a bridge between batch processing logic and Kafka repository.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    ConsumeServiceImpl                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │   ┌─────────────────────────────────────────────────────────┐   │
//! │   │              KafkaRepository (injected)                 │   │
//! │   │                                                         │   │
//! │   │   - consume_messages(topic, max) -> Vec<Value>          │   │
//! │   │   - consume_one(topic) -> Option<Value>                 │   │
//! │   └─────────────────────────────────────────────────────────┘   │
//! │                              │                                  │
//! │                              ▼                                  │
//! │   ┌─────────────────────────────────────────────────────────┐   │
//! │   │              ConsumeService Methods                      │   │
//! │   │                                                         │   │
//! │   │   - consume_messages() : Delegates to KafkaRepository   │   │
//! │   │   - consume_one()      : Delegates to KafkaRepository   │   │
//! │   └─────────────────────────────────────────────────────────┘   │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! The service is used by batch jobs to consume messages from Kafka topics:
//!
//! ```rust,no_run
//! // In batch processing
//! let messages = consume_service.consume_messages("my_topic", 100).await?;
//! for msg in messages {
//!     // Process each message
//! }
//! ```

use crate::common::*;

use crate::repository::kafka_repository::*;
use crate::service_trait::consume_service::*;

/// Concrete implementation of the consume service.
///
/// `ConsumeServiceImpl` wraps a `KafkaRepository` and provides
/// message consumption functionality to the batch processing layer.
///
/// # Type Parameters
///
/// * `K` - A type implementing `KafkaRepository` trait
///
/// # Fields
///
/// * `kafka_conn` - The injected Kafka repository for message consumption
#[derive(Debug, Getters, Clone, new)]
pub struct ConsumeServiceImpl<K: KafkaRepository> {
    /// Kafka repository for consuming messages.
    kafka_conn: Arc<K>,
}

#[async_trait]
impl<K> ConsumeService for ConsumeServiceImpl<K>
where
    K: KafkaRepository + Sync + Send,
{
    /// Consumes messages from a Kafka topic.
    ///
    /// Delegates to the underlying `KafkaRepository` to consume messages.
    /// Each topic gets its own consumer with independent offset tracking.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to retrieve
    ///
    /// # Returns
    ///
    /// Returns a vector of JSON values representing the consumed messages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// let messages = service.consume_messages("orders_topic", 50).await?;
    /// info!("Consumed {} messages", messages.len());
    /// ```
    async fn consume_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<Value>, anyhow::Error> {
        info!(
            "[ConsumeServiceImpl::consume_messages] Consuming from topic: {}, max: {}",
            topic, max_messages
        );

        let messages: Vec<Value> = self
            .kafka_conn
            .consume_messages(topic, max_messages)
            .await?;

        info!(
            "[ConsumeServiceImpl::consume_messages] Consumed {} messages from topic: {}",
            messages.len(),
            topic
        );

        Ok(messages)
    }

    /// Consumes a single message from a Kafka topic.
    ///
    /// Convenience method for retrieving just one message.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    ///
    /// # Returns
    ///
    /// Returns `Some(Value)` if a message is available, `None` otherwise.
    async fn consume_one(&self, topic: &str) -> Result<Option<Value>, anyhow::Error> {
        info!(
            "[ConsumeServiceImpl::consume_one] Consuming single message from topic: {}",
            topic
        );

        let message = self.kafka_conn.consume_one(topic).await?;

        if message.is_some() {
            info!(
                "[ConsumeServiceImpl::consume_one] Successfully consumed one message from topic: {}",
                topic
            );
        } else {
            info!(
                "[ConsumeServiceImpl::consume_one] No message available from topic: {}",
                topic
            );
        }

        Ok(message)
    }

    async fn consume_messages_with_group(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> Result<Vec<Value>, anyhow::Error> {
        info!(
            "[ConsumeServiceImpl::consume_messages_with_group] Consuming from topic: {}, max: {}, group_suffix: {}",
            topic, max_messages, group_suffix
        );

        let messages: Vec<Value> = self
            .kafka_conn
            .consume_messages_with_group(topic, max_messages, group_suffix)
            .await?;

        info!(
            "[ConsumeServiceImpl::consume_messages_with_group] Consumed {} messages from topic: {} (group: {})",
            messages.len(),
            topic,
            group_suffix
        );

        Ok(messages)
    }

    async fn consume_messages_as_with_group<T>(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        T: DeserializeOwned,
    {
        info!(
            "[ConsumeServiceImpl::consume_messages_as_with_group] Consuming from topic: {}, max: {}, group_suffix: {}",
            topic, max_messages, group_suffix
        );

        let messages: Vec<Value> = self
            .kafka_conn
            .consume_messages_with_group(topic, max_messages, group_suffix)
            .await?;

        info!(
            "[ConsumeServiceImpl::consume_messages_as_with_group] Consumed {} messages from topic: {} (group: {}), deserializing...",
            messages.len(),
            topic,
            group_suffix
        );

        let mut results: Vec<T> = Vec::with_capacity(messages.len());

        for (index, msg) in messages.into_iter().enumerate() {
            let deserialized: T = serde_json::from_value(msg)
                .inspect_err(|e| {
                    error!("[ConsumeServiceImpl::consume_messages_as_with_group] Failed to deserialize message at index {} from topic: {} (group: {}): {:#}", index, topic, group_suffix, e);
                })?;
            results.push(deserialized);
        }

        info!(
            "[ConsumeServiceImpl::consume_messages_as_with_group] Successfully deserialized {} messages from topic: {} (group: {})",
            results.len(),
            topic,
            group_suffix
        );

        Ok(results)
    }

    /// Consumes messages from a Kafka topic and deserializes them into type T.
    ///
    /// This is a generic version of `consume_messages` that automatically
    /// deserializes JSON messages into the specified type.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Target type for deserialization (must implement `DeserializeOwned`)
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to retrieve
    ///
    /// # Returns
    ///
    /// Returns a vector of deserialized messages of type `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Kafka consumption fails
    /// - JSON deserialization fails for any message
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// #[derive(Deserialize)]
    /// struct OrderMessage {
    ///     order_id: i64,
    ///     amount: f64,
    /// }
    ///
    /// let orders: Vec<OrderMessage> = service
    ///     .consume_messages_as("orders_topic", 50)
    ///     .await?;
    /// ```
    async fn consume_messages_as<T>(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        T: DeserializeOwned,
    {
        info!(
            "[ConsumeServiceImpl::consume_messages_as] Consuming from topic: {}, max: {}",
            topic, max_messages
        );

        let messages: Vec<Value> = self
            .kafka_conn
            .consume_messages(topic, max_messages)
            .await?;

        info!(
            "[ConsumeServiceImpl::consume_messages_as] Consumed {} messages from topic: {}, deserializing...",
            messages.len(),
            topic
        );

        let mut results: Vec<T> = Vec::with_capacity(messages.len());

        for (index, msg) in messages.into_iter().enumerate() {
            let deserialized: T = serde_json::from_value(msg)
                .inspect_err(|e| {
                    error!("[ConsumeServiceImpl::consume_messages_as] Failed to deserialize message at index {} from topic: {}: {:#}", index, topic, e);
                })?;
            results.push(deserialized);
        }

        info!(
            "[ConsumeServiceImpl::consume_messages_as] Successfully deserialized {} messages from topic: {}",
            results.len(),
            topic
        );

        Ok(results)
    }

    /// Replicates committed offsets from one consumer group to another for a given topic.
    ///
    /// Reads the committed offsets of `source_group` and applies them to `target_group`,
    /// allowing `target_group` to resume consumption from the exact same position as `source_group`.
    ///
    /// # Arguments
    ///
    /// * `topic`        - The Kafka topic whose offsets are being replicated
    /// * `source_group` - The consumer group to read offsets from
    /// * `target_group` - The consumer group to apply offsets to
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if offsets were successfully replicated.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Topic metadata fetch fails
    /// - Source group committed offset fetch fails
    /// - Target group offset commit fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// service
    ///     .replicate_consumer_group_offsets("orders_topic", "batch-group", "replay-group")
    ///     .await?;
    /// // "replay-group" now starts consuming from where "batch-group" left off
    /// ```
    async fn replicate_consumer_group_offsets(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> anyhow::Result<()> {
        self.kafka_conn
            .copy_consumer_group_offsets(topic, source_group, target_group)
            .await
            .inspect_err(|e| {
                error!("[ConsumeServiceImpl::replicate_consumer_group_offsets] Failed to copy consumer group offsets from source to target. {:#}", e);
            })
    }

    async fn get_consumer_group_lag(
        &self,
        topic: &str,
        reference_group: &str,
        catchup_group: &str,
    ) -> anyhow::Result<i64> {
        let ref_offset: i64 = self
            .kafka_conn
            .get_committed_offsets_total(topic, reference_group)
            .await
            .inspect_err(|e| {
                error!("[ConsumeServiceImpl::get_consumer_group_lag] Failed to get reference offset for '{}': {:#}", reference_group, e);
            })?;

        let catchup_offset: i64 = self
            .kafka_conn
            .get_committed_offsets_total(topic, catchup_group)
            .await
            .inspect_err(|e| {
                error!("[ConsumeServiceImpl::get_consumer_group_lag] Failed to get catchup offset for '{}': {:#}", catchup_group, e);
            })?;

        Ok((ref_offset - catchup_offset).max(0))
    }

    async fn get_consumer_group_lag_by_partition(
        &self,
        topic: &str,
        reference_group: &str,
        catchup_group: &str,
    ) -> anyhow::Result<ConsumerGroupLag> {
        info!(
            "[ConsumeServiceImpl::get_consumer_group_lag_by_partition] Calculating lag for topic '{}': reference='{}', catchup='{}'",
            topic, reference_group, catchup_group
        );

        // Fetch per-partition offsets for both groups
        let ref_offsets: HashMap<i32, i64> = self
            .kafka_conn
            .get_committed_offsets_by_partition(topic, reference_group)
            .await
            .inspect_err(|e| {
                error!(
                    "[ConsumeServiceImpl::get_consumer_group_lag_by_partition] Failed to get reference offsets for '{}': {:#}",
                    reference_group, e
                );
            })?;

        let catchup_offsets: HashMap<i32, i64> = self
            .kafka_conn
            .get_committed_offsets_by_partition(topic, catchup_group)
            .await
            .inspect_err(|e| {
                error!(
                    "[ConsumeServiceImpl::get_consumer_group_lag_by_partition] Failed to get catchup offsets for '{}': {:#}",
                    catchup_group, e
                );
            })?;

        // Calculate per-partition lag
        let mut partition_lags: Vec<PartitionLag> = Vec::new();
        let mut total_lag: i64 = 0;

        // Get all unique partition IDs from both groups
        let mut all_partitions: HashSet<i32> = ref_offsets.keys().copied().collect(); // partition 번호의 집합 -> 0,1,2..
        all_partitions.extend(catchup_offsets.keys().copied());
        /*
            extend() 는 기존 Set에 새로운 값들을 추가하는 것. (HashSet 은 중복을 허용하지 않기 때문.)
            {0, 1, 2}.extend(0, 1, 3) -> {0, 1, 2, 3}
        */
        
        // Sort partition IDs for consistent ordering -> Hash Set 은 순서가 없으므로 정렬을 해서 벡터로 변환하고 싶은것.
        let mut sorted_partitions: Vec<i32> = all_partitions.into_iter().collect();
        sorted_partitions.sort();

        for partition_id in sorted_partitions {
            let ref_offset: i64 = *ref_offsets.get(&partition_id).unwrap_or(&0);
            let catchup_offset: i64 = *catchup_offsets.get(&partition_id).unwrap_or(&0);
            let lag: i64 = (ref_offset - catchup_offset).max(0);

            partition_lags.push(PartitionLag {
                partition: partition_id,
                reference_offset: ref_offset,
                catchup_offset,
                lag,
            });

            total_lag += lag;
        }

        let result: ConsumerGroupLag = ConsumerGroupLag {
            topic: topic.to_string(),
            reference_group: reference_group.to_string(),
            catchup_group: catchup_group.to_string(),
            partition_lags,
            total_lag,
        };

        info!(
            "[ConsumeServiceImpl::get_consumer_group_lag_by_partition] Calculated lag for topic '{}': total_lag={}, partitions={}",
            topic, result.total_lag, result.partition_lags.len()
        );

        Ok(result)
    }
}

impl<K> ConsumeServiceImpl<K> where K: KafkaRepository + Sync + Send {}
