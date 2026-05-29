use crate::common::*;

use super::KafkaRepositoryImpl;

impl KafkaRepositoryImpl {
    /// Serializes `payload` to JSON and sends it to `topic`.
    pub(super) async fn send_message(
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

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map(|_| ())
            .map_err(|(e, _)| {
                anyhow!(
                    "[KafkaRepositoryImpl::send_message] Failed to send to topic {}: {:?}",
                    topic,
                    e
                )
            })
    }
}
