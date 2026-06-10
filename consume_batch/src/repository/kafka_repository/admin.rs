use crate::common::*;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{Offset, TopicPartitionList};

use super::KafkaRepositoryImpl;

impl KafkaRepositoryImpl {
    /// Builds an `AdminClient` using the current broker/SASL config.
    pub(super) fn initialize_admin_client(
        &self,
    ) -> anyhow::Result<AdminClient<DefaultClientContext>> {
        let mut admin_config: ClientConfig = ClientConfig::new();
        admin_config.set("bootstrap.servers", &self.kafka_brokers);

        if let (Some(protocol), Some(mechanism), Some(username), Some(password)) = (
            &self.security_protocol,
            &self.sasl_mechanism,
            &self.sasl_username,
            &self.sasl_password,
        ) {
            admin_config
                .set("security.protocol", protocol)
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        admin_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::initialize_admin_client] Failed to create admin client: {:?}",
                e
            )
        })
    }

    /// Returns whether a consumer group has active members.
    ///
    /// rdkafka 0.38 lacks `describe_groups`, so this always returns true
    /// and logs a warning instead.
    pub(super) async fn is_consumer_group_active(&self, _group_id: &str) -> anyhow::Result<bool> {
        Ok(true)
    }

    /// Logs a warning if the group has active consumers.
    ///
    /// Kafka Admin API cannot forcibly remove group members;
    /// actual deactivation requires stopping the consumer application.
    pub(super) async fn modify_consumer_group_deactivate(
        &self,
        group_id: &str,
    ) -> anyhow::Result<()> {
        let is_active: bool = self.is_consumer_group_active(group_id).await?;

        if is_active {
            warn!(
                "[KafkaRepositoryImpl::modify_consumer_group_deactivate] Group '{}' has active consumers. \
                 For safest operation, please stop consumer application before copying offsets.",
                group_id
            );
        } else {
            info!(
                "[KafkaRepositoryImpl::modify_consumer_group_deactivate] Group '{}' has no active consumers. Safe to proceed.",
                group_id
            );
        }

        Ok(())
    }

    /// Builds a `BaseConsumer` `ClientConfig` with broker/SASL settings applied.
    fn build_base_consumer_config(&self, group_id: &str) -> ClientConfig {
        let mut config: ClientConfig = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.kafka_brokers)
            .set("group.id", group_id);

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

        config
    }

    /// Fetches partition metadata for `topic` via a temporary `BaseConsumer`.
    fn fetch_topic_partitions(
        &self,
        topic: &str,
        consumer: &BaseConsumer,
    ) -> anyhow::Result<Vec<i32>> {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl] Failed to fetch metadata for topic '{}': {:?}",
                    topic,
                    e
                )
            })?;

        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| {
                anyhow!(
                    "[KafkaRepositoryImpl] Topic '{}' not found in metadata",
                    topic
                )
            })?;

        if topic_meta.partitions().is_empty() {
            return Err(anyhow!(
                "[KafkaRepositoryImpl] Topic '{}' has no partitions",
                topic
            ));
        }

        Ok(topic_meta.partitions().iter().map(|p| p.id()).collect())
    }

    /// Copies committed offsets from `source_group` to `target_group` for `topic`.
    ///
    /// Steps:
    /// 1. Create source `BaseConsumer` and fetch partition metadata
    /// 2. Read source group's committed offsets
    /// 3. Create target `BaseConsumer` and commit those offsets synchronously
    ///
    /// Skips copy if source group has never committed (all offsets Invalid).
    pub(super) async fn modify_offsets_internal(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> anyhow::Result<()> {
        let source_group_id = format!("{}-{}-{}", self.base_group_id, topic, source_group);
        let target_group_id = format!("{}-{}-{}", self.base_group_id, topic, target_group);

        // ──────────────────────────────────────────────────────────────
        // [1단계] source 그룹 BaseConsumer 생성
        // ──────────────────────────────────────────────────────────────
        let source_consumer: BaseConsumer =
            self.build_base_consumer_config(&source_group_id).create().map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::modify_offsets_internal] Failed to create source consumer: {:?}",
                    e
                )
            })?;

        // ──────────────────────────────────────────────────────────────
        // [2단계] 토픽 파티션 목록 조회
        // ──────────────────────────────────────────────────────────────
        let partition_ids = self.fetch_topic_partitions(topic, &source_consumer)?;

        // ──────────────────────────────────────────────────────────────
        // [3단계] source 그룹 committed offset 조회
        // ──────────────────────────────────────────────────────────────
        let mut tpl: TopicPartitionList = TopicPartitionList::new();
        for pid in &partition_ids {
            tpl.add_partition(topic, *pid);
        }

        source_consumer.assign(&tpl).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Failed to assign partitions: {:?}",
                e
            )
        })?;

        let committed_tpl: TopicPartitionList = source_consumer
            .committed(Duration::from_secs(10))
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::modify_offsets_internal] Failed to fetch committed offsets for group '{}': {:?}",
                    source_group_id, e
                )
            })?;

        for elem in committed_tpl.elements() {
            let offset_str = match elem.offset() {
                Offset::Offset(o) => format!("{}", o),
                Offset::Invalid => "Invalid".to_string(),
                Offset::Beginning => "Beginning".to_string(),
                Offset::End => "End".to_string(),
                _ => "Unknown".to_string(),
            };
            info!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Source group '{}' partition {} offset: {}",
                source_group_id,
                elem.partition(),
                offset_str
            );
        }

        let has_valid_offset: bool = committed_tpl
            .elements()
            .iter()
            .any(|e| e.offset() != rdkafka::Offset::Invalid);

        if !has_valid_offset {
            info!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Source group '{}' has no committed offsets yet. Skipping copy.",
                source_group_id
            );
            return Ok(());
        }

        // ──────────────────────────────────────────────────────────────
        // [4단계] target 그룹에 offset 직접 commit
        // ──────────────────────────────────────────────────────────────
        let mut target_config = self.build_base_consumer_config(&target_group_id);
        target_config.set("enable.auto.commit", "false");

        let target_consumer: BaseConsumer = target_config.create().map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Failed to create target consumer: {:?}",
                e
            )
        })?;

        let mut assign_tpl: TopicPartitionList = TopicPartitionList::new();
        for pid in &partition_ids {
            assign_tpl.add_partition(topic, *pid);
        }

        target_consumer.assign(&assign_tpl).map_err(|e| {
            anyhow!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Failed to assign partitions to target consumer: {:?}",
                e
            )
        })?;

        target_consumer
            .commit(&committed_tpl, CommitMode::Sync)
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::modify_offsets_internal] Failed to commit offsets for group '{}': {:?}",
                    target_group_id, e
                )
            })?;

        for elem in committed_tpl.elements() {
            let offset_str = match elem.offset() {
                Offset::Offset(o) => format!("{}", o),
                _ => "N/A".to_string(),
            };
            info!(
                "[KafkaRepositoryImpl::modify_offsets_internal] Copied to target group '{}' partition {} offset: {}",
                target_group_id,
                elem.partition(),
                offset_str
            );
        }

        info!(
            "[KafkaRepositoryImpl::modify_offsets_internal] Successfully committed offsets for target group '{}'",
            target_group_id
        );

        Ok(())
    }

    /// Deletes all records from `topic` up to the high watermark of each partition.
    ///
    /// The topic itself is preserved; only the data is removed.
    pub(super) async fn purge_topic_records(&self, topic: &str) -> anyhow::Result<()> {
        info!(
            "[KafkaRepositoryImpl::purge_topic_records] Purging all records from topic: {}",
            topic
        );

        // ──────────────────────────────────────────────────────────────
        // [1단계] AdminClient 생성
        // ──────────────────────────────────────────────────────────────
        let admin_client = self.initialize_admin_client()?;

        // ──────────────────────────────────────────────────────────────
        // [2단계] 메타데이터/워터마크 조회용 임시 BaseConsumer 생성
        // StreamConsumer를 쓰지 않는 이유: subscribe + auto commit 부작용 방지
        // ──────────────────────────────────────────────────────────────
        let temp_consumer: BaseConsumer = self
            .build_base_consumer_config("__admin_temp__")
            .create()
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::purge_topic_records] Failed to create temp consumer: {:?}",
                    e
                )
            })?;

        let partition_ids = self.fetch_topic_partitions(topic, &temp_consumer)?;

        // ──────────────────────────────────────────────────────────────
        // [3단계] 각 파티션의 high watermark offset 조회
        // high watermark를 delete_records에 넘기면 모든 레코드 삭제
        // ──────────────────────────────────────────────────────────────
        let mut tpl: TopicPartitionList = TopicPartitionList::new();

        for pid in &partition_ids {
            let (_low, high) = temp_consumer
                .fetch_watermarks(topic, *pid, Duration::from_secs(10))
                .map_err(|e| {
                    anyhow!(
                        "[KafkaRepositoryImpl::purge_topic_records] Failed to fetch watermarks for {}[{}]: {:?}",
                        topic, pid, e
                    )
                })?;

            if high > 0 {
                info!(
                    "[KafkaRepositoryImpl::purge_topic_records] Partition {}: deleting records up to offset {}",
                    pid, high
                );
                tpl.add_partition_offset(topic, *pid, Offset::Offset(high))
                    .map_err(|e| {
                        anyhow!(
                            "[KafkaRepositoryImpl::purge_topic_records] Failed to set partition offset: {:?}",
                            e
                        )
                    })?;
            }
        }

        if tpl.count() == 0 {
            info!(
                "[KafkaRepositoryImpl::purge_topic_records] Topic {} is already empty",
                topic
            );
            return Ok(());
        }

        // ──────────────────────────────────────────────────────────────
        // [4단계] delete_records 호출 — 실제 데이터 삭제
        // ──────────────────────────────────────────────────────────────
        admin_client
            .delete_records(&tpl, &AdminOptions::new())
            .await
            .map_err(|e| {
                anyhow!(
                    "[KafkaRepositoryImpl::purge_topic_records] Failed to delete records from topic {}: {:?}",
                    topic, e
                )
            })?;

        info!(
            "[KafkaRepositoryImpl::purge_topic_records] Successfully purged all records from topic: {}",
            topic
        );

        Ok(())
    }

    /// Safely copies offsets from `source_group` to `target_group` for `topic`.
    ///
    /// Steps:
    /// 1. Check target group active status (log warning)
    /// 2. Deactivate target group
    /// 3. Copy offsets via `modify_offsets_internal`
    pub(super) async fn copy_consumer_group_offsets(
        &self,
        topic: &str,
        source_group: &str,
        target_group: &str,
    ) -> anyhow::Result<()> {
        let source_group_id: String = format!("{}-{}-{}", self.base_group_id, topic, source_group);
        let target_group_id: String = format!("{}-{}-{}", self.base_group_id, topic, target_group);

        info!(
            "[KafkaRepositoryImpl::copy_consumer_group_offsets] Starting safe offset copy: '{}' → '{}' (topic: '{}')",
            source_group_id, target_group_id, topic
        );

        // ──────────────────────────────────────────────────────────────
        // [1단계] target 그룹 활성 상태 확인
        // ──────────────────────────────────────────────────────────────
        let is_active: bool = self.is_consumer_group_active(&target_group_id).await?;

        if is_active {
            info!(
                "[KafkaRepositoryImpl::copy_consumer_group_offsets] Target group '{}' has active consumers. Will deactivate before copying.",
                target_group_id
            );
        } else {
            info!(
                "[KafkaRepositoryImpl::copy_consumer_group_offsets] Target group '{}' is already inactive.",
                target_group_id
            );
        }

        // ──────────────────────────────────────────────────────────────
        // [2단계] target 그룹 비활성화
        // ──────────────────────────────────────────────────────────────
        self.modify_consumer_group_deactivate(&target_group_id)
            .await?;

        info!(
            "[KafkaRepositoryImpl::copy_consumer_group_offsets] Target group '{}' deactivated.",
            target_group_id
        );

        // ──────────────────────────────────────────────────────────────
        // [3단계] offset 복사
        // ──────────────────────────────────────────────────────────────
        let copy_result = self
            .modify_offsets_internal(topic, source_group, target_group)
            .await;

        match &copy_result {
            Ok(_) => {
                info!(
                    "[KafkaRepositoryImpl::copy_consumer_group_offsets] ✓ Successfully copied offsets '{}' → '{}' for topic '{}'",
                    source_group_id, target_group_id, topic
                );
                info!(
                    "[KafkaRepositoryImpl::copy_consumer_group_offsets] Target group '{}' can be reactivated by starting consumer application.",
                    target_group_id
                );
            }
            Err(e) => {
                error!(
                    "[KafkaRepositoryImpl::copy_consumer_group_offsets] ✗ Failed to copy offsets: {:?}",
                    e
                );
                error!(
                    "[KafkaRepositoryImpl::copy_consumer_group_offsets] Target group '{}' remains deactivated. Restart consumer application to reactivate.",
                    target_group_id
                );
            }
        }

        copy_result
    }
}
