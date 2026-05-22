#![allow(dead_code)]
//! Kafka repository implementation with shared instance pattern.
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
//! │  HashMap<Topic, StreamConsumer>  │  FutureProducer              │
//! │  (One Consumer per Topic)        │  (Shared for all topics)     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//!
//! | Module        | Responsibility                                          |
//! |---------------|---------------------------------------------------------|
//! | `consumer`    | Consumer creation, message polling                      |
//! | `producer`    | Message publishing                                      |
//! | `admin`       | Topic purge, consumer group offset management           |
//! | `offset`      | Committed offset queries                                |

mod admin;
mod consumer;
mod offset;
mod producer;

use crate::app_config::*;
use crate::common::*;

/// Trait defining Kafka repository operations.
#[async_trait]
pub trait KafkaRepository: Send + Sync {
    /// Consumes messages from a Kafka topic with a specified limit.
    async fn find_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<Value>, anyhow::Error>;

    /// Consumes messages from a Kafka topic with a custom consumer group suffix.
    ///
    /// Full group ID format: `{base_group_id}-{topic}-{group_suffix}`
    async fn find_messages_by_group(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> Result<Vec<Value>, anyhow::Error>;

    /// Consumes a single message from a Kafka topic.
    async fn find_one(&self, topic: &str) -> Result<Option<Value>, anyhow::Error>;

    /// Sends a JSON message to a Kafka topic.
    async fn input_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> Result<(), anyhow::Error>;

    /// Purges all records from a Kafka topic using the Admin API's delete_records.
    async fn delete_topic_records(&self, topic: &str) -> Result<(), anyhow::Error>;

    /// Copies committed offsets from source consumer group to target consumer group.
    ///
    /// Safely deactivates target group before copying, then copies source offsets.
    ///
    /// # Safety
    ///
    /// Deactivates target group first to prevent committed offset race conditions.
    async fn modify_consumer_group_offsets(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> Result<(), anyhow::Error>;

    /// Returns total committed offset summed across all partitions for a consumer group.
    ///
    /// Full group ID format: `{base_group_id}-{topic}-{group_suffix}`
    async fn find_committed_offsets_total(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<i64>;

    /// Returns committed offsets per partition for a consumer group.
    ///
    /// Full group ID format: `{base_group_id}-{topic}-{group_suffix}`
    async fn find_committed_offsets_by_partition(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<HashMap<i32, i64>>;
}

/// Concrete implementation of the Kafka repository (consumer and producer).
///
/// Manages topic-specific consumers (one per topic, keyed by topic+group_suffix)
/// and a single shared producer. Thread-safe via `RwLock` on consumers.
pub struct KafkaRepositoryImpl {
    consumers: Arc<RwLock<HashMap<String, Arc<StreamConsumer>>>>,
    producer: FutureProducer,
    kafka_brokers: String,
    base_group_id: String,
    security_protocol: Option<String>,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
}

impl KafkaRepositoryImpl {
    pub fn new() -> anyhow::Result<Self> {
        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!("[KafkaRepositoryImpl::new] app_config: {:#}", e);
        })?;

        let kafka_brokers: String = app_config.kafka_host().to_string();
        let base_group_id: String = app_config.kafka_group_id().to_string();
        let security_protocol: Option<String> = app_config.kafka_security_protocol().clone();
        let sasl_mechanism: Option<String> = app_config.kafka_sasl_mechanism().clone();
        let sasl_username: Option<String> = app_config.kafka_sasl_username().clone();
        let sasl_password: Option<String> = app_config.kafka_sasl_password().clone();

        let mut producer_config: ClientConfig = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", &kafka_brokers)
            .set("message.timeout.ms", "30000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("batch.num.messages", "10000");

        if let Some(protocol) = &security_protocol {
            producer_config.set("security.protocol", protocol);
        }

        if let (Some(mechanism), Some(username), Some(password)) =
            (&sasl_mechanism, &sasl_username, &sasl_password)
        {
            producer_config
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        let producer: FutureProducer = producer_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::new] Failed to create producer: {:?}",
                e
            )
        })?;

        let sasl_enabled: bool =
            sasl_mechanism.is_some() && sasl_username.is_some() && sasl_password.is_some();
        info!(
            "[KafkaRepositoryImpl::new] Kafka repository initialized (brokers: {}, base_group: {}, security_protocol: {}, sasl: {})",
            kafka_brokers,
            base_group_id,
            security_protocol.as_deref().unwrap_or("PLAINTEXT"),
            if sasl_enabled { "enabled" } else { "disabled" }
        );

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
}

#[async_trait]
impl KafkaRepository for KafkaRepositoryImpl {
    async fn find_messages(&self, topic: &str, max_messages: usize) -> anyhow::Result<Vec<Value>> {
        self.consume_messages(topic, None, max_messages).await
    }

    async fn find_messages_by_group(
        &self,
        topic: &str,
        max_messages: usize,
        group_suffix: &str,
    ) -> anyhow::Result<Vec<Value>> {
        self.consume_messages(topic, Some(group_suffix), max_messages).await
    }

    async fn find_one(&self, topic: &str) -> anyhow::Result<Option<Value>> {
        Ok(self.consume_messages(topic, None, 1).await?.into_iter().next())
    }

    async fn input_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> anyhow::Result<()> {
        self.send_message(topic, key, payload).await
    }

    async fn delete_topic_records(&self, topic: &str) -> anyhow::Result<()> {
        self.purge_topic_records(topic).await
    }

    async fn modify_consumer_group_offsets(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> anyhow::Result<()> {
        self.copy_consumer_group_offsets(topic, source_group, target_group).await
    }

    async fn find_committed_offsets_total(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<i64> {
        self.fetch_committed_offsets_total(topic, group_suffix).await
    }

    async fn find_committed_offsets_by_partition(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<HashMap<i32, i64>> {
        self.fetch_committed_offsets_by_partition(topic, group_suffix).await
    }
}
