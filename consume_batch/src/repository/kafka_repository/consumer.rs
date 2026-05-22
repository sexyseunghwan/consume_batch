use crate::common::*;

use super::KafkaRepositoryImpl;

impl KafkaRepositoryImpl {
    /// Gets existing consumer or creates a new topic-specific one (double-checked locking).
    ///
    /// Consumer key: `{topic}` or `{topic}-{group_suffix}`.
    /// Group ID: `{base_group_id}-{topic}` or `{base_group_id}-{topic}-{group_suffix}`.
    pub(super) async fn find_or_create_consumer(
        &self,
        topic: &str,
        group_suffix: Option<&str>,
    ) -> anyhow::Result<Arc<StreamConsumer>> {
        let consumer_key: String = match group_suffix {
            Some(suffix) => format!("{}-{}", topic, suffix),
            None => topic.to_string(),
        };

        {
            let consumers = self.consumers.read().await;
            if let Some(consumer) = consumers.get(&consumer_key) {
                return Ok(Arc::clone(consumer));
            }
        }

        let mut consumers = self.consumers.write().await;

        // Double-check after acquiring write lock
        if let Some(consumer) = consumers.get(&consumer_key) {
            return Ok(Arc::clone(consumer));
        }

        let group_id: String = match group_suffix {
            Some(suffix) => format!("{}-{}-{}", self.base_group_id, topic, suffix),
            None => format!("{}-{}", self.base_group_id, topic),
        };

        info!(
            "[KafkaRepositoryImpl::find_or_create_consumer] Creating consumer for topic: {} (group.id: {})",
            topic, group_id
        );

        let mut client_config: ClientConfig = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.kafka_brokers)
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set("session.timeout.ms", "6000");

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
                "[KafkaRepositoryImpl::find_or_create_consumer] SASL enabled (protocol: {}, mechanism: {}, user: {})",
                protocol, mechanism, username
            );
        }

        let consumer: StreamConsumer = client_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::find_or_create_consumer] Failed to create consumer for topic {}: {:?}",
                topic, e
            )
        })?;

        consumer.subscribe(&[topic]).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::find_or_create_consumer] Failed to subscribe to topic {}: {:?}",
                topic, e
            )
        })?;

        let consumer_arc: Arc<StreamConsumer> = Arc::new(consumer);
        consumers.insert(consumer_key.clone(), Arc::clone(&consumer_arc));

        info!(
            "[KafkaRepositoryImpl::find_or_create_consumer] Consumer created and subscribed to topic: {}",
            topic
        );

        Ok(consumer_arc)
    }

    /// Consumes up to `max_messages` messages from `topic`.
    ///
    /// If `group_suffix` is Some, uses a dedicated consumer group for that suffix,
    /// allowing independent offset tracking for the same topic.
    /// Times out after 5 seconds of no messages.
    pub(super) async fn consume_messages(
        &self,
        topic: &str,
        group_suffix: Option<&str>,
        max_messages: usize,
    ) -> anyhow::Result<Vec<Value>> {
        let consumer: Arc<StreamConsumer> =
            self.find_or_create_consumer(topic, group_suffix).await?;

        let mut messages: Vec<Value> = Vec::new();
        let mut stream: MessageStream<'_, rdkafka::consumer::DefaultConsumerContext> =
            consumer.stream();

        while messages.len() < max_messages {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(Ok(borrowed_message))) => {
                    if let Some(payload) = borrowed_message.payload() {
                        let payload_str: &str =
                            std::str::from_utf8(payload).map_err(|e| {
                                anyhow!(
                                    "[KafkaRepositoryImpl::consume_messages] Invalid UTF-8 in topic {}: {:?}",
                                    topic, e
                                )
                            })?;

                        let json_value: Value =
                            serde_json::from_str(payload_str).map_err(|e| {
                                anyhow!(
                                    "[KafkaRepositoryImpl::consume_messages] JSON parse error in topic {}: {:?}",
                                    topic, e
                                )
                            })?;

                        messages.push(json_value);
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!(
                        "[KafkaRepositoryImpl::consume_messages] Kafka error on topic {}: {:?}",
                        topic, e
                    );
                }
                Ok(None) => {
                    info!(
                        "[KafkaRepositoryImpl::consume_messages] Stream ended for topic: {}",
                        topic
                    );
                    break;
                }
                Err(_) => {
                    info!(
                        "[KafkaRepositoryImpl::consume_messages] Timeout for topic: {}, consumed {} messages",
                        topic,
                        messages.len()
                    );
                    break;
                }
            }
        }

        info!(
            "[KafkaRepositoryImpl::consume_messages] Finished {} messages from topic: {}",
            messages.len(),
            topic
        );

        Ok(messages)
    }
}
