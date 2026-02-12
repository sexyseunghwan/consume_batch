use crate::common::*;

use crate::repository::kafka_repository::*;

use crate::service_trait::producer_service::*;

#[derive(Debug, Getters, Clone, new)]
pub struct ProducerServiceImpl<K: KafkaRepository> {
    kafka_conn: Arc<K>,
}

#[async_trait]
impl<K> ProducerService for ProducerServiceImpl<K>
where
    K: KafkaRepository + Send + Sync,
{
    #[doc = "Produce JSON message to specified Kafka topic"]
    async fn produce_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> Result<(), anyhow::Error> {
        self.kafka_conn.send_message(topic, key, payload).await
    }

    #[doc = "Produce a single serializable object to a specific Kafka topic"]
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `object` - Serializable object to send
    /// * `key` - Optional message key
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if message sent successfully
    async fn produce_object_to_topic<T>(
        &self,
        topic: &str,
        object: &T,
        key: Option<&str>,
    ) -> Result<(), anyhow::Error>
    where
        T: Serialize + Send + Sync,
    {
        // Serialize object to JSON Value
        let json_value: Value = serde_json::to_value(object).map_err(|e| {
            anyhow!(
                "[ProducerServiceImpl::produce_object_to_topic] Failed to serialize object: {:?}",
                e
            )
        })?;

        // Send message
        self.kafka_conn
            .send_message(topic, key, &json_value)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ProducerServiceImpl::produce_object_to_topic] Failed to send message: {:?}",
                    e
                )
            })?;

        info!(
            "[ProducerServiceImpl::produce_object_to_topic] Successfully sent message to topic: {}",
            topic
        );

        Ok(())
    }

    #[doc = "Produce multiple serializable objects to a specific Kafka topic"]
    /// Each object will be serialized to JSON and sent as a separate message
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `objects` - Slice of serializable objects
    /// * `key_fn` - Optional function to generate key for each object
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if all messages sent successfully
    ///
    /// # Example
    /// ```
    /// // Example 1: Send without keys
    /// let spent_details = vec![spent1, spent2, spent3];
    /// producer.produce_objects_to_topic("dev_spent_detail", &spent_details, None).await?;
    ///
    /// // Example 2: Send with user-based keys (for ordering)
    /// producer.produce_objects_to_topic(
    ///     "dev_spent_detail",
    ///     &spent_details,
    ///     Some(&|obj| format!("user:{}", obj.user_seq))
    /// ).await?;
    /// ```
    async fn produce_objects_to_topic<T, F>(
        &self,
        topic: &str,
        objects: &[T],
        key_fn: Option<F>,
    ) -> Result<(), anyhow::Error>
    where
        T: Serialize + Send + Sync,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        if objects.is_empty() {
            error!("No objects to send to topic: {}", topic);
            return Ok(());
        }

        info!("Producing {} objects to topic: {}", objects.len(), topic);

        let mut success_count: i32 = 0;

        for (idx, obj) in objects.iter().enumerate() {
            // Serialize object to JSON Value
            let json_value: Value = serde_json::to_value(obj).map_err(|e| {
                anyhow!(
                    "[ProducerServiceImpl::produce_objects_to_topic] Failed to serialize object at index {}: {:?}",
                    idx,
                    e
                )
            })?;

            // Generate key if key_fn is provided
            let key: Option<String> = key_fn.as_ref().map(|f| f(obj));

            // Send message
            match self
                .kafka_conn
                .send_message(topic, key.as_deref(), &json_value)
                .await
            {
                Ok(_) => {
                    success_count += 1;
                    if success_count % 100 == 0 {
                        info!(
                            "[ProducerServiceImpl::produce_objects_to_topic] Sent {} messages to {}",
                            success_count, topic
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "[ProducerServiceImpl::produce_objects_to_topic] Failed to send object at index {}: {:?}",
                        idx, e
                    );
                    return Err(anyhow!("Failed to send object at index {}: {}", idx, e));
                }
            }
        }

        info!(
            "[ProducerServiceImpl::produce_objects_to_topic] Completed producing {} objects to topic: {}",
            success_count, topic
        );

        Ok(())
    }
}
