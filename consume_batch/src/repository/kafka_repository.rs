//! Kafka repository implementation with shared instance pattern.
//!
//! This module provides the data access layer for Apache Kafka consumer operations
//! designed to be shared across services via Arc for efficient resource management.
//!
//! # Architecture
//!
//! ```text
//!                    ┌────────────────────────────┐
//!                    │         main()             │
//!                    │  kafka_repo = Arc::new()   │
//!                    └────────────┬───────────────┘
//!                                 │
//!                 ┌───────────────┼───────────────┐
//!                 │               │               │
//!            Arc::clone      Arc::clone      Arc::clone
//!                 │               │               │
//!                 ▼               ▼               ▼
//!         ConsumeService   ProducerService  BatchService
//!                 │               │               │
//!                 └───────────────┼───────────────┘
//!                                 │
//!                                 ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │        Shared KafkaRepositoryImpl (Single Instance)             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │           ┌───────────────────────────────────────┐             │
//! │           │   HashMap<Topic, StreamConsumer>      │             │
//! │           │   (One Consumer per Topic)            │             │
//! │           └───────────────────────────────────────┘             │
//! │                          │                                      │
//! │                          ▼                                      │
//! │           ┌─────────────────────────────────┐                   │
//! │           │    get_or_create_consumer       │                   │
//! │           │    (Topic-specific consumer)    │                   │
//! │           └─────────────────────────────────┘                   │
//! │                          │                                      │
//! │            ┌─────────────┼─────────────┐                        │
//! │            ▼             ▼             ▼                        │
//! │      Consumer #1   Consumer #2   Consumer #3                    │
//! │      (topic_1)     (topic_2)     (topic_3)                      │
//! │      group.id:     group.id:     group.id:                      │
//! │      "grp-topic1"  "grp-topic2"  "grp-topic3"                   │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Shared Instance Pattern
//!
//! Uses `Arc` (Atomic Reference Counting) for safe shared ownership:
//! - **Single Instance**: Only one `KafkaRepositoryImpl` created in main()
//! - **Shared Ownership**: Multiple services hold `Arc` references to same instance
//! - **Efficient**: `Arc::clone()` only increments reference count (cheap operation)
//! - **Thread-Safe**: Internal state protected by `RwLock`
//! - **No Accidental Copies**: `Clone` trait removed to prevent expensive deep copies
//!
//! # Consumer Strategy
//!
//! Each topic gets its own dedicated consumer with a unique group.id:
//! - **Isolation**: Each topic's offset is tracked independently
//! - **Parallel Safe**: Multiple batch jobs can consume different topics concurrently
//! - **Group ID Format**: `{base_group_id}-{topic_name}`
//!
//! # Consumer Configuration (per topic)
//!
//! Each consumer is configured with:
//! - **Auto offset reset**: earliest (start from beginning if no offset)
//! - **Enable auto commit**: true (automatic offset commits)
//! - **Session timeout**: 6 seconds
//!
//! # Environment Variables
//!
//! | Variable         | Description                      | Example                          |
//! |------------------|----------------------------------|----------------------------------|
//! | `KAFKA_HOST`     | Comma-separated list of brokers  | `broker1:9092,broker2:9092`      |
//! | `KAFKA_GROUP_ID` | Base consumer group ID           | `my-batch-consumer`              |
//!
//! # Usage Example
//!
//! ```rust,no_run
//! // In main.rs - create single instance
//! let kafka_repo = Arc::new(KafkaRepositoryImpl::new()?);
//!
//! // Share with services (Arc::clone is cheap - only increments reference count)
//! let consume_service = ConsumeServiceImpl::new(Arc::clone(&kafka_repo));
//! let producer_service = ProducerServiceImpl::new(Arc::clone(&kafka_repo));
//! let batch_service = BatchServiceImpl::new(
//!     mysql_service,
//!     elastic_service,
//!     consume_service,
//!     producer_service,
//! );
//!
//! // All services share the same KafkaRepositoryImpl instance
//! // Connection pools and consumers are reused efficiently
//! ```
use crate::app_config::*;
use crate::common::*;
use std::ops::Deref;

/// Trait defining Kafka repository operations.
///
/// This trait abstracts Kafka consumer and producer operations,
/// allowing for different implementations (e.g., mock implementations for testing).
///
/// # Implementors
///
/// - [`KafkaRepositoryImpl`] - Production implementation with real Kafka client
#[async_trait]
pub trait KafkaRepository: Send + Sync {
    /// Consumes messages from a Kafka topic with a specified limit.
    ///
    /// Creates or reuses a topic-specific consumer with its own group.id,
    /// ensuring independent offset tracking per topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to consume
    ///
    /// # Returns
    ///
    /// Returns `Ok(Vec<Value>)` containing the consumed messages as JSON values.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Consumer creation fails
    /// - Topic subscription fails
    /// - Message deserialization fails
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

    /// Sends a JSON message to a Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to send to
    /// * `key` - Optional message key for partitioning
    /// * `payload` - The JSON payload to send
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful delivery.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Message serialization fails
    /// - Producer fails to send message
    async fn send_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> Result<(), anyhow::Error>;
}

/// Concrete implementation of the Kafka repository (consumer and producer).
///
/// `KafkaRepositoryImpl` manages multiple topic-specific consumers
/// for parallel batch processing and a shared producer for publishing messages.
///
/// # Consumer Management
///
/// Uses a `HashMap<String, StreamConsumer>` to manage topic-specific consumers:
/// - Each topic gets its own consumer instance
/// - Each consumer has a unique group.id: `{base_group_id}-{topic_name}`
/// - Offsets are tracked independently per topic
/// - Thread-safe access via `RwLock`
///
/// # Producer Management
///
/// Uses a single shared `FutureProducer` for all topics:
/// - One producer instance handles all message sending
/// - Thread-safe and can be shared across async tasks
/// - `FutureProducer.clone()` is cheap - it internally uses `Arc<ThreadedProducer>`
/// - No explicit `Arc` wrapper needed - rdkafka already handles it internally
///
/// # Thread Safety
///
/// - **Consumers**: Protected by `RwLock` for safe concurrent access
/// - **Producer**: Inherently thread-safe via internal `Arc`
///   - `FutureProducer` implements `Clone` with internal `Arc<ThreadedProducer>`
///   - Cloning only increments reference count (cheap operation)
///   - Multiple clones share the same underlying Kafka producer connection
///
/// # Examples
///
/// ```rust,no_run
/// use crate::repository::KafkaRepositoryImpl;
///
/// let kafka_repo = KafkaRepositoryImpl::new()?;
///
/// // These can run in parallel - each gets its own consumer
/// let repo1 = kafka_repo.clone();
/// let repo2 = kafka_repo.clone();
///
/// let handle1 = tokio::spawn(async move {
///     repo1.consume_messages("topic_1", 100).await
/// });
/// let handle2 = tokio::spawn(async move {
///     repo2.consume_messages("topic_2", 100).await
/// });
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct KafkaRepositoryImpl {
    /// Topic-specific consumers for independent offset tracking.
    ///
    /// Key: topic name, Value: dedicated StreamConsumer for that topic.
    /// Protected by RwLock for thread-safe access.
    consumers: Arc<RwLock<HashMap<String, Arc<StreamConsumer>>>>,

    /// Shared Kafka producer for sending messages.
    /// Thread-safe and can be cloned/shared across tasks.
    producer: FutureProducer,

    /// Kafka broker addresses for creating new consumers.
    kafka_brokers: String,

    /// Base group ID prefix for consumer groups.
    /// Actual group.id will be: `{base_group_id}-{topic_name}`
    base_group_id: String,

    /// Security protocol (e.g., "SASL_PLAINTEXT", "SASL_SSL")
    security_protocol: Option<String>,

    /// SASL mechanism (e.g., "PLAIN", "SCRAM-SHA-256")
    sasl_mechanism: Option<String>,

    /// SASL username
    sasl_username: Option<String>,

    /// SASL password
    sasl_password: Option<String>,
}

// Removed Clone implementation - use Arc<KafkaRepositoryImpl> instead
// This prevents accidental full clones and makes it clear that
// the instance should be shared via Arc::clone()

impl KafkaRepositoryImpl {
    /// Creates a new `KafkaRepositoryImpl` instance.
    ///
    /// Initializes the consumer map. Consumers are created lazily
    /// when `consume_messages` is called for a specific topic.
    ///
    /// # Returns
    ///
    /// Returns `Ok(KafkaRepositoryImpl)` on successful initialization.
    pub fn new() -> anyhow::Result<Self> {
        // Load configuration from environment
        let app_config: &AppConfig = AppConfig::global();

        let kafka_brokers: String = app_config.kafka_host().to_string();
        let base_group_id: String = app_config.kafka_group_id().to_string();
        let security_protocol: Option<String> = app_config.kafka_security_protocol().clone();
        let sasl_mechanism: Option<String> = app_config.kafka_sasl_mechanism().clone();
        let sasl_username: Option<String> = app_config.kafka_sasl_username().clone();
        let sasl_password: Option<String> = app_config.kafka_sasl_password().clone();

        // Create producer configuration
        let mut producer_config: ClientConfig = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", &kafka_brokers)
            .set("message.timeout.ms", "30000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("batch.num.messages", "10000");

        // Apply security protocol if configured
        if let Some(protocol) = &security_protocol {
            producer_config.set("security.protocol", protocol);
        }

        // Apply SASL configuration if enabled
        if let (Some(mechanism), Some(username), Some(password)) =
            (&sasl_mechanism, &sasl_username, &sasl_password)
        {
            producer_config
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        // Create producer
        let producer: FutureProducer = producer_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::new] Failed to create producer: {:?}",
                e
            )
        })?;

        // info!(
        //     "[KafkaRepositoryImpl::new] Kafka repository initialized (brokers: {}, base_group: {}, sasl: {})",
        //     kafka_brokers, base_group_id, kafka_configs.is_sasl_enabled()
        // );

        Ok(KafkaRepositoryImpl {
            consumers: Arc::new(RwLock::new(HashMap::new())),
            producer,
            kafka_brokers,
            base_group_id,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            sasl_password,
        })
    }

    /// Gets or creates a consumer for a specific topic.
    ///
    /// If a consumer for the topic already exists, returns the existing one.
    /// Otherwise, creates a new consumer with a topic-specific group.id.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name to get/create a consumer for
    ///
    /// # Returns
    ///
    /// Returns an Arc to the StreamConsumer for the topic.
    ///
    /// # Consumer Configuration
    ///
    /// | Setting                | Value                        | Description                |
    /// |------------------------|------------------------------|----------------------------|
    /// | `bootstrap.servers`    | from config                  | Kafka broker addresses     |
    /// | `group.id`             | `{base_group_id}-{topic}`    | Topic-specific group       |
    /// | `auto.offset.reset`    | earliest                     | Start from earliest        |
    /// | `enable.auto.commit`   | true                         | Auto commit offsets        |
    /// | `session.timeout.ms`   | 6000                         | 6 second session timeout   |
    async fn get_or_create_consumer(&self, topic: &str) -> anyhow::Result<Arc<StreamConsumer>> {
        // First, try to get existing consumer with read lock
        {
            let consumers: tokio::sync::RwLockReadGuard<'_, HashMap<String, Arc<StreamConsumer>>> =
                self.consumers.read().await;

            if let Some(consumer) = consumers.get(topic) {
                return Ok(Arc::clone(consumer));
            }
        }

        // Consumer doesn't exist, create new one with write lock
        let mut consumers: tokio::sync::RwLockWriteGuard<'_, HashMap<String, Arc<StreamConsumer>>> =
            self.consumers.write().await;

        // Double-check after acquiring write lock (another thread might have created it)
        if let Some(consumer) = consumers.get(topic) {
            return Ok(Arc::clone(consumer));
        }

        // Create topic-specific group.id
        let group_id: String = format!("{}-{}", self.base_group_id, topic);

        info!(
            "[KafkaRepositoryImpl::get_or_create_consumer] Creating new consumer for topic: {} (group.id: {})",
            topic, group_id
        );

        // Create new consumer for this topic
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.kafka_brokers)
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("session.timeout.ms", "6000");

        // Apply SASL configuration if enabled
        if let (Some(protocol), Some(mechanism), Some(username), Some(password)) = (
            &self.security_protocol,
            &self.sasl_mechanism,
            &self.sasl_username,
            &self.sasl_password,
        ) {
            client_config
                .set("security.protocol", protocol)
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);

            info!(
                "[KafkaRepositoryImpl::get_or_create_consumer] SASL enabled (protocol: {}, mechanism: {}, user: {})",
                protocol, mechanism, username
            );
        }

        let consumer: StreamConsumer = client_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::get_or_create_consumer] Failed to create consumer for topic {}: {:?}",
                topic,
                e
            )
        })?;

        // Subscribe to the topic
        consumer.subscribe(&[topic]).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::get_or_create_consumer] Failed to subscribe to topic {}: {:?}",
                topic,
                e
            )
        })?;

        let consumer_arc: Arc<StreamConsumer> = Arc::new(consumer);
        consumers.insert(topic.to_string(), Arc::clone(&consumer_arc));

        info!(
            "[KafkaRepositoryImpl::get_or_create_consumer] Consumer created and subscribed to topic: {}",
            topic
        );

        Ok(consumer_arc)
    }
}

#[async_trait]
impl KafkaRepository for KafkaRepositoryImpl {
    /// Consumes messages from a Kafka topic with a specified limit.
    ///
    /// Gets or creates a topic-specific consumer, ensuring independent
    /// offset tracking for each topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    /// * `max_messages` - Maximum number of messages to consume
    ///
    /// # Returns
    ///
    /// Returns a vector of JSON values representing the consumed messages.
    async fn consume_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> anyhow::Result<Vec<Value>> {
        // Get or create topic-specific consumer
        let consumer: Arc<StreamConsumer> = self.get_or_create_consumer(topic).await?;

        let mut messages: Vec<Value> = Vec::new();
        let mut stream: MessageStream<'_, rdkafka::consumer::DefaultConsumerContext> =
            consumer.stream();

        info!(
            "[KafkaRepositoryImpl::consume_messages] Starting to consume from topic: {} (max: {})",
            topic, max_messages
        );

        // Consume messages up to the limit
        while messages.len() < max_messages {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(result)) => match result {
                    Ok(borrowed_message) => {
                        if let Some(payload) = borrowed_message.payload() {
                            let payload_str: &str = std::str::from_utf8(payload).map_err(|e| {
                                anyhow!(
                                    "[KafkaRepositoryImpl::consume_messages] Invalid UTF-8: {:?}",
                                    e
                                )
                            })?;

                            let json_value: Value =
                                serde_json::from_str(payload_str).map_err(|e| {
                                    anyhow!(
                                        "[KafkaRepositoryImpl::consume_messages] JSON parse error: {:?}",
                                        e
                                    )
                                })?;

                            messages.push(json_value);

                            info!(
                                "[KafkaRepositoryImpl::consume_messages] Consumed message {}/{} from topic: {}, partition: {}, offset: {}",
                                messages.len(),
                                max_messages,
                                topic,
                                borrowed_message.partition(),
                                borrowed_message.offset()
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            "[KafkaRepositoryImpl::consume_messages] Error consuming message from {}: {:?}",
                            topic, e
                        );
                    }
                },
                Ok(None) => {
                    info!(
                        "[KafkaRepositoryImpl::consume_messages] Stream ended for topic: {}",
                        topic
                    );
                    break;
                }
                Err(_) => {
                    info!(
                        "[KafkaRepositoryImpl::consume_messages] Timeout reached for topic: {}, consumed {} messages",
                        topic,
                        messages.len()
                    );
                    break;
                }
            }
        }

        info!(
            "[KafkaRepositoryImpl::consume_messages] Finished consuming {} messages from topic: {}",
            messages.len(),
            topic
        );

        Ok(messages)
    }

    /// Consumes a single message from a Kafka topic.
    ///
    /// Convenience method for consuming just one message.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to consume from
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(Value))` if a message is available within timeout,
    /// `Ok(None)` if no message is available.
    async fn consume_one(&self, topic: &str) -> anyhow::Result<Option<Value>> {
        let messages: Vec<Value> = self.consume_messages(topic, 1).await?;
        Ok(messages.into_iter().next())
    }

    /// Sends a JSON message to a Kafka topic.
    ///
    /// Uses the shared producer to send messages asynchronously.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic to send to
    /// * `key` - Optional message key for partitioning
    /// * `payload` - The JSON payload to send
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful delivery.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Message serialization fails
    /// - Producer fails to send message within timeout (30 seconds)
    async fn send_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> anyhow::Result<()> {
        let payload_str: String = serde_json::to_string(payload).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::send_message] Failed to serialize payload: {:?}",
                e
            )
        })?;

        let mut record: FutureRecord<'_, str, String> =
            FutureRecord::to(topic).payload(&payload_str);

        if let Some(k) = key {
            record = record.key(k);
        }

        match self.producer.send(record, Duration::from_secs(30)).await {
            Ok(delivery) => {
                info!(
                    "[KafkaRepositoryImpl::send_message] Message sent to topic: {}, partition: {}, offset: {}",
                    topic, delivery.partition, delivery.offset
                );
                Ok(())
            }
            Err((e, _)) => {
                let error_message: String = format!(
                    "[KafkaRepositoryImpl::send_message] Failed to send message to topic {}: {:?}",
                    topic, e
                );
                Err(anyhow!(error_message))
            }
        }
    }
}
