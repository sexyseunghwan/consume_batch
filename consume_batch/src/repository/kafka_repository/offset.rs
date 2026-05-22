use crate::common::*;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};

use super::KafkaRepositoryImpl;

impl KafkaRepositoryImpl {
    /// Builds and assigns a `BaseConsumer` to all partitions of `topic`, returns committed TPL.
    ///
    /// Full group ID: `{base_group_id}-{topic}-{group_suffix}`
    async fn build_committed_tpl(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<(String, TopicPartitionList)> {
        let group_id = format!("{}-{}-{}", self.base_group_id, topic, group_suffix);

        let mut config: ClientConfig = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.kafka_brokers)
            .set("group.id", &group_id);

        if let (Some(protocol), Some(mechanism), Some(username), Some(password)) = (
            &self.security_protocol,
            &self.sasl_mechanism,
            &self.sasl_username,
            &self.sasl_password,
        ) {
            config
                .set("security.protocol", protocol)
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        let consumer: BaseConsumer = config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::build_committed_tpl] Failed to create consumer for group '{}': {:?}",
                group_id, e
            )
        })?;

        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::build_committed_tpl] Failed to fetch metadata for topic '{}': {:?}",
                    topic, e
                )
            })?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| {
                anyhow!(
                    "[KafkaRepositoryImpl::build_committed_tpl] Topic '{}' not found in metadata",
                    topic
                )
            })?;

        let mut tpl: TopicPartitionList = TopicPartitionList::new();
        for partition in topic_metadata.partitions() {
            tpl.add_partition(topic, partition.id());
        }

        consumer.assign(&tpl).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::build_committed_tpl] Failed to assign partitions: {:?}",
                e
            )
        })?;

        let committed_tpl: TopicPartitionList = consumer
            .committed(Duration::from_secs(10))
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::build_committed_tpl] Failed to fetch committed offsets for group '{}': {:?}",
                    group_id, e
                )
            })?;

        Ok((group_id, committed_tpl))
    }

    /// Returns total committed offset summed across all partitions.
    ///
    /// Partitions with no committed offset count as 0.
    pub(super) async fn fetch_committed_offsets_total(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<i64> {
        let (group_id, committed_tpl) = self.build_committed_tpl(topic, group_suffix).await?;

        if committed_tpl.count() == 0 {
            return Ok(0);
        }

        let total: i64 = committed_tpl
            .elements()
            .iter()
            .map(|elem| match elem.offset() {
                Offset::Offset(o) => o,
                _ => 0,
            })
            .sum();

        info!(
            "[KafkaRepositoryImpl::fetch_committed_offsets_total] group='{}' topic='{}' total_offset={}",
            group_id, topic, total
        );

        Ok(total)
    }

    /// Returns committed offsets per partition.
    ///
    /// Partitions with no committed offset are set to 0.
    pub(super) async fn fetch_committed_offsets_by_partition(
        &self,
        topic: &str,
        group_suffix: &str,
    ) -> anyhow::Result<HashMap<i32, i64>> {
        let (group_id, committed_tpl) = self.build_committed_tpl(topic, group_suffix).await?;

        if committed_tpl.count() == 0 {
            return Ok(HashMap::new());
        }

        let mut partition_offsets: HashMap<i32, i64> = HashMap::new();

        for elem in committed_tpl.elements() {
            let offset: i64 = match elem.offset() {
                Offset::Offset(o) => o,
                _ => 0,
            };
            partition_offsets.insert(elem.partition(), offset);
        }

        info!(
            "[KafkaRepositoryImpl::fetch_committed_offsets_by_partition] group='{}' topic='{}' partition_offsets={:?}",
            group_id, topic, partition_offsets
        );

        Ok(partition_offsets)
    }
}
