use crate::common::*;

#[async_trait]
pub trait ProducerService {
    async fn produce_message(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &Value,
    ) -> Result<(), anyhow::Error>;
    /// Produce a single serializable object to a specific Kafka topic
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `object` - Serializable object to send
    /// * `key` - Optional message key
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if message sent successfully
    ///
    /// # Example
    /// ```
    /// let spent_detail = SpentDetail { ... };
    /// producer.produce_object_to_topic("dev_spent_detail", &spent_detail, None).await?;
    /// ```
    async fn produce_object_to_topic<T>(
        &self,
        topic: &str,
        object: &T,
        key: Option<&str>,
    ) -> Result<(), anyhow::Error>
    where
        T: Serialize + Send + Sync;

    /// Produce multiple serializable objects to a specific Kafka topic
    /// Each object in the list will be serialized to JSON and sent as a separate message
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `objects` - List of serializable objects to send
    /// * `key_fn` - Optional function to generate key for each object
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if all messages sent successfully
    ///
    /// # Example
    /// ```
    /// let spent_details = vec![spent1, spent2, spent3];
    ///
    /// // Without key
    /// producer.produce_objects_to_topic::<_, fn(&SpentDetail) -> String>("dev_spent_detail", &spent_details, None).await?;
    ///
    /// // With key function
    /// producer.produce_objects_to_topic(
    ///     "dev_spent_detail",
    ///     &spent_details,
    ///     Some(|obj: &SpentDetail| format!("user:{}", obj.user_seq))
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
        F: Fn(&T) -> String + Send + Sync + 'static;

    /// Purge all records from a specific Kafka topic.
    ///
    /// Uses the Admin API's delete_records to remove all messages
    /// up to each partition's high watermark offset.
    /// The topic itself is preserved — only the data is deleted.
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name to purge
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if records were successfully deleted
    async fn purge_topic(&self, topic: &str) -> Result<(), anyhow::Error>;
}
