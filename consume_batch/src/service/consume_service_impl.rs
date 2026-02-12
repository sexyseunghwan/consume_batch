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
}
