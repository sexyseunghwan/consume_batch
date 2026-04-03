use crate::batch_log;
use crate::common::*;
use crate::enums::IndexingType;
use crate::global_state::*;
use crate::models::{
    ConsumerGroupLag, SpentDetailWithRelations, SpentDetailWithRelationsEs, SpentTypeKeyword,
    batch_schedule::BatchScheduleItem,
};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService,
};

// ============================================================================
// Struct
// ============================================================================
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService,
    P: ProducerService,
    E: ElasticService,
    C: ConsumeService,
{
    mysql_service: Arc<M>,
    producer_service: Arc<P>,
    elastic_service: Arc<E>,
    consume_service: Arc<C>,
}

impl<M, P, E, C> IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    /// Creates a new `IndexingServiceImpl` with the given service dependencies.
    ///
    /// All services are shared via `Arc`, allowing them to be reused across
    /// `IndexingServiceImpl` and other services (e.g., `BatchServiceImpl`)
    /// without cloning the underlying instances.
    ///
    /// # Arguments
    ///
    /// * `mysql_service`    - MySQL service for fetching `SpentDetail` records
    /// * `producer_service` - Kafka producer service for publishing messages
    /// * `elastic_service`  - Elasticsearch service for indexing and alias management
    /// * `consume_service`  - Kafka consumer service for reading indexed messages
    pub fn new(
        mysql_service: Arc<M>,
        producer_service: Arc<P>,
        elastic_service: Arc<E>,
        consume_service: Arc<C>,
    ) -> Self {
        Self {
            mysql_service,
            producer_service,
            elastic_service,
            consume_service,
        }
    }

    // ============================================================================
    // Private helpers
    // ============================================================================

    /// Migrates all `SpentDetail` records from MySQL to a Kafka topic.
    ///
    /// Purges the target topic first to ensure a clean state, then fetches
    /// all records from MySQL in batches and publishes each record as a JSON
    /// message to the configured Kafka topic.
    ///
    /// # Steps
    ///
    /// 1. Purge the target Kafka topic (`relation_topic`)
    /// 2. Fetch records from MySQL in batches using `offset` + `batch_size`
    /// 3. Serialize each record to JSON and produce it to the topic
    /// 4. Repeat until no more records are returned
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (topic name, batch size, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Topic purge fails
    /// - JSON serialization of a record fails
    async fn migrate_to_kafka(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        let produce_topic: &str = schedule_item.relation_topic().as_str();
        let batch_size: u64 = *schedule_item.batch_size() as u64;

        batch_log!(
            info,
            "[IndexingServiceImpl::migrate_to_kafka] Starting. batch_size={}",
            batch_size
        );

        let mut offset: u64 = 0;
        let mut total_selected: usize = 0;

        self.producer_service
            .purge_topic(produce_topic)
            .await
            .inspect_err(|e| {
                error!("[IndexingServiceImpl::migrate_to_kafka]: {:#}", e);
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::migrate_to_kafka] All data for topic `{}` has been deleted.",
            produce_topic
        );

        loop {
            let rows: Vec<SpentDetailWithRelations> = match self
                .mysql_service
                .fetch_spent_details_for_indexing(offset, batch_size)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    batch_log!(
                        error,
                        "[IndexingServiceImpl::migrate_to_kafka] Failed to load from DB: {:?}",
                        e
                    );
                    continue;
                }
            };

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let json: Value = serde_json::to_value(row).inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::migrate_to_kafka] JSON serialize failed: {:#}",
                        e
                    );
                })?;

                match self
                    .producer_service
                    .produce_object_to_topic(produce_topic, &json, None)
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        batch_log!(
                            error,
                            "[IndexingServiceImpl::migrate_to_kafka] Kafka produce failed: {:#}",
                            e
                        );
                        break;
                    }
                }
            }

            offset += batch_size;
            total_selected += rows.len();
        }

        batch_log!(
            info,
            "[IndexingServiceImpl::migrate_to_kafka] Done. produced={} rows",
            total_selected
        );

        Ok(())
    }

    /// Consumes the full dataset from the primary Kafka topic and bulk-indexes it into Elasticsearch.
    ///
    /// Reads messages in batches from `relation_topic` until the topic is exhausted,
    /// converting each message to [`SpentDetailWithRelationsEs`] and indexing it into
    /// `new_index_name`. Also tracks the maximum `produced_at` timestamp across all
    /// consumed messages via global state, which is used by the dynamic catch-up phase
    /// to coordinate incremental indexing timing.
    ///
    /// # Arguments
    ///
    /// * `schedule_item`   - The batch schedule configuration (topic, batch size, consumer group, etc.)
    /// * `new_index_name`  - The target Elasticsearch index to write into
    ///
    /// # Returns
    ///
    /// Returns the total number of documents indexed on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Kafka message consumption fails
    /// - Elasticsearch bulk index fails
    async fn process_spent_detail_static(
        &self,
        schedule_item: &BatchScheduleItem,
        new_index_name: &str,
    ) -> anyhow::Result<u64> {
        let relation_topic: &str = schedule_item.relation_topic();
        let batch_size: usize = *schedule_item.batch_size();
        let consumer_group: &str = schedule_item.consumer_group();

        let mut total_indexed: u64 = 0;

        loop {
            let messages: Vec<SpentDetailWithRelations> = self
                .consume_service
                .consume_messages_as_with_group(relation_topic, batch_size, consumer_group)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_static] Failed to consume: {:#}",
                        e
                    );
                })?;

            if messages.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_static] No more messages in '{}', finishing",
                    relation_topic
                );
                break;
            }

            let batch_count: usize = messages.len();

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_static] Consumed {} messages",
                batch_count
            );

            //let max_produced_at: Option<DateTime<Utc>> = messages.iter().map(|m| m.produced_at).max();
            //set_max_static_spent_detail_index_timestamp(max_produced_at).await;

            let es_messages: Vec<SpentDetailWithRelationsEs> =
                messages.into_iter().map(|m| m.into()).collect();

            self.elastic_service
                .bulk_index(new_index_name, es_messages)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_static] bulk_index failed: {:#}",
                        e
                    );
                })?;

            total_indexed += batch_count as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_static] Indexed {} docs so far",
                total_indexed
            );
        }

        Ok(total_indexed)
    }

    /// Processes incremental messages from the catch-up topic and indexes them into the new index.
    ///
    /// This phase ensures that changes produced during the full static indexing window
    /// are not lost. It runs after the static phase completes and monitors the offset lag
    /// between the reference group and the catch-up group per partition.
    ///
    /// # Steps
    ///
    /// 1. Poll lag between `consumer_group` (reference) and `consumer_group_sub` (catch-up)
    /// 2. When `lag <= batch_size`, pause the live incremental indexer to prevent split-brain writes
    /// 3. Continue consuming from the catch-up topic until `lag == 0`
    /// 4. Swap `write_alias` and `read_alias` to the new index
    /// 5. Resume the live incremental indexer
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (topics, consumer groups, index alias, etc.)
    /// * `index_name`    - The new Elasticsearch index being built
    ///
    /// # Returns
    ///
    /// Returns the total number of catch-up documents indexed on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Consumer group lag query fails
    /// - Kafka message consumption fails
    /// - Elasticsearch bulk operations fail
    /// - Alias swap fails
    async fn process_spent_detail_dynamic(
        &self,
        schedule_item: &BatchScheduleItem,
        index_name: &str,
    ) -> anyhow::Result<u64> {
        let relation_topic: &str = schedule_item.relation_topic_sub();
        let batch_size: usize = *schedule_item.batch_size();
        let consumer_group: &str = schedule_item.consumer_group();
        let consumer_group_sub: &str = schedule_item.consumer_group_sub();
        let base_alias: &str = schedule_item.index_name();
        let write_alias: String = format!("write_{}", base_alias);
        let read_alias: String = format!("read_{}", base_alias);

        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_detail_dynamic] Starting. topic='{}', index='{}', ref='{}', catchup='{}'",
            relation_topic,
            index_name,
            consumer_group,
            consumer_group_sub
        );

        let mut total_processed: u64 = 0;
        let mut indexing_paused: bool = false;

        loop {
            let lag_info: ConsumerGroupLag = self
                .consume_service
                .get_consumer_group_lag_by_partition(
                    relation_topic,
                    consumer_group,
                    consumer_group_sub,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_dynamic] Failed to get lag: {:#}",
                        e
                    );
                })?;

            let lag: i64 = lag_info.total_lag;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_dynamic] lag={}, batch_size={}, partitions={}",
                lag,
                batch_size,
                lag_info.partition_lags.len()
            );

            for partition_lag in &lag_info.partition_lags {
                if partition_lag.lag > 0 {
                    info!(
                        "[IndexingServiceImpl::process_spent_detail_dynamic] Partition {}: lag={} (ref={}, catchup={})",
                        partition_lag.partition,
                        partition_lag.lag,
                        partition_lag.reference_offset,
                        partition_lag.catchup_offset
                    );
                }
            }

            if !indexing_paused && lag <= batch_size as i64 {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_dynamic] Almost caught up (lag={}). Pausing incremental indexing.",
                    lag
                );

                // 현재 실시간으로 동작하고 있는 증분색인을 잠시 멈춰준다.
                set_spent_detail_indexing(false).await;
                indexing_paused = true;
            }

            if indexing_paused && lag == 0 {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_dynamic] Fully caught up. total_processed={}. Swapping aliases.",
                    total_processed
                );

                // 현재 인덱스 설정정보 원복해준다 -> refresh, replication
                self.elastic_service
                    .revert_index_setting(index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_dynamic] Failed to revert the index settings.{:#}",
                            e
                        );
                    })?;

                self.elastic_service
                    .update_write_alias(&write_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_dynamic] update_write_alias failed: {:#}",
                            e
                        );
                    })?;

                self.elastic_service
                    .update_read_alias(&read_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_dynamic] update_read_alias failed: {:#}",
                            e
                        );
                    })?;

                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_dynamic] Alias swap complete. Resuming incremental indexing."
                );

                // 실시간 증분색인 재개
                set_spent_detail_indexing(true).await;

                // consumer-lag 가 더이상 없으므로 loop 종료
                break;
            }

            let messages: Vec<SpentDetailWithRelations> = self
                .consume_service
                .consume_messages_as_with_group(relation_topic, batch_size, consumer_group_sub)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_dynamic] consume failed: {:#}",
                        e
                    );
                })?;

            let batch_count: usize = messages.len();

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            let (to_insert, to_update, to_delete) = Self::partition_by_indexing_type(messages);
            Self::apply_es_operations(
                &self.elastic_service,
                index_name,
                to_insert,
                to_update,
                to_delete,
            )
            .await?;

            total_processed += batch_count as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_dynamic] Indexed {} catch-up docs (total={})",
                batch_count,
                total_processed
            );
        }

        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_detail_dynamic] Done. total_processed={}",
            total_processed
        );

        Ok(total_processed)
    }

    /// Partitions a batch of messages into insert, update, and delete buckets by `indexing_type`.
    ///
    /// Consumes the input vector and routes each message to the appropriate bucket
    /// based on its [`IndexingType`] variant. Delete messages are reduced to their
    /// `spent_idx` only; insert and update messages are converted to [`SpentDetailWithRelationsEs`].
    ///
    /// # Arguments
    ///
    /// * `messages` - The batch of messages to partition
    ///
    /// # Returns
    ///
    /// A tuple of `(to_insert, to_update, to_delete)` where `to_delete` contains
    /// only the `spent_idx` values of records to be removed.
    fn partition_by_indexing_type(
        messages: Vec<SpentDetailWithRelations>,
    ) -> (
        Vec<SpentDetailWithRelationsEs>,
        Vec<SpentDetailWithRelationsEs>,
        Vec<i64>,
    ) {
        let mut to_insert: Vec<SpentDetailWithRelationsEs> = Vec::new();
        let mut to_update: Vec<SpentDetailWithRelationsEs> = Vec::new();
        let mut to_delete: Vec<i64> = Vec::new();

        for msg in messages {
            match msg.indexing_type {
                IndexingType::Insert => to_insert.push(msg.into()),
                IndexingType::Update => to_update.push(msg.into()),
                IndexingType::Delete => to_delete.push(msg.spent_idx),
            }
        }

        (to_insert, to_update, to_delete)
    }

    /// Applies bulk insert, update, and delete operations to the given Elasticsearch index.
    ///
    /// Each operation is only executed if its corresponding bucket is non-empty,
    /// so callers do not need to check for emptiness before calling this function.
    ///
    /// # Arguments
    ///
    /// * `elastic_service` - Elasticsearch service for bulk operations
    /// * `index_name`      - The target index name (or alias) to operate on
    /// * `to_insert`       - Documents to bulk insert
    /// * `to_update`       - Documents to bulk update (matched by `spent_idx`)
    /// * `to_delete`       - `spent_idx` values of documents to bulk delete
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the bulk operations fail.
    async fn apply_es_operations(
        elastic_service: &Arc<E>,
        index_name: &str,
        to_insert: Vec<SpentDetailWithRelationsEs>,
        to_update: Vec<SpentDetailWithRelationsEs>,
        to_delete: Vec<i64>,
    ) -> anyhow::Result<()> {
        if !to_insert.is_empty() {
            batch_log!(
                info,
                "[IndexingServiceImpl::apply_es_operations] Inserting {} docs into '{}'",
                to_insert.len(),
                index_name
            );
            elastic_service
                .bulk_index(index_name, to_insert)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::apply_es_operations] bulk_index failed: {:#}",
                        e
                    );
                })?;
        }

        if !to_update.is_empty() {
            batch_log!(
                info,
                "[IndexingServiceImpl::apply_es_operations] Updating {} docs in '{}'",
                to_update.len(),
                index_name
            );
            elastic_service
                .bulk_update(index_name, to_update, "spent_idx")
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::apply_es_operations] bulk_update failed: {:#}",
                        e
                    );
                })?;
        }

        if !to_delete.is_empty() {
            batch_log!(
                info,
                "[IndexingServiceImpl::apply_es_operations] Deleting {} docs from '{}'",
                to_delete.len(),
                index_name
            );
            elastic_service
                .bulk_delete(index_name, to_delete)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::apply_es_operations] bulk_delete failed: {:#}",
                        e
                    );
                })?;
        }

        Ok(())
    }

    /// Performs full indexing of spent type keywords from MySQL into Elasticsearch.
    ///
    /// Fetches all [`SpentTypeKeyword`] records from MySQL in batches and bulk-indexes
    /// them into a newly created index. After indexing is complete, the index settings
    /// are finalized and the alias is atomically swapped to the new index.
    /// The old index is deleted after the swap.
    ///
    /// # Steps
    ///
    /// 1. Create a new index with bulk-optimized settings via `prepare_full_index`
    /// 2. Fetch records from MySQL in batches and bulk-index into the new index
    /// 3. Finalize index settings and swap the alias to the new index
    /// 4. Delete the old (unused) index
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (index name, batch size, mapping schema, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Index creation or preparation fails
    /// - MySQL batch fetch fails
    /// - Elasticsearch bulk indexing fails
    /// - Index finalization or alias swap fails
    /// - Old index deletion fails
    async fn process_spent_type_full(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let batch_size: usize = *schedule_item.batch_size();

        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_type_full] Processing {} (index: {})",
            schedule_item.batch_name(),
            index_alias
        );

        let old_indexies: Vec<String> = self
            .elastic_service
            .get_index_name_by_alias(index_alias)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::process_spent_type_full] Failed to find any existing indices associated with the specified alias: {:#}",
                    e
                )
            })?;
        
        
        let new_index_name: String = self
            .elastic_service
            .prepare_full_index(index_alias, schedule_item.mapping_schema())
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::process_spent_type_full] prepare_full_index: {:#}",
                    e
                );
            })?;
        
        println!("new_index_name: {:?}", new_index_name);

        let mut offset: u64 = 0;
        let mut total_indexed: u64 = 0;

        loop {
            
            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_type_full] Fetching batch at offset={}, batch_size={}",
                offset,
                batch_size
            );

            let keywords: Vec<SpentTypeKeyword> = self
                .mysql_service
                .fetch_spent_type_keywords_batch(offset, batch_size as u64)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_type_full] Failed to fetch keywords: {:#}",
                        e
                    );
                })?;

            if keywords.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_type_full] No more data to index"
                );
                break;
            }

            let batch_count: usize = keywords.len();

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_type_full] Indexing {} documents",
                batch_count
            );

            self.elastic_service
                .bulk_index(&new_index_name, keywords)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_type_full] bulk_index failed: {:#}",
                        e
                    );
                })?;

            total_indexed += batch_count as u64;
            offset += batch_size as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_type_full] Indexed {} documents so far",
                total_indexed
            );
        }

        self.elastic_service
            .revert_index_setting(&new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::process_spent_type_full] Failed to revert the index settings.: {:#}",
                    e
                )
            })?;
        
        self.elastic_service
            .swap_alias(index_alias, &new_index_name)
            .await
            .inspect_err(|e| {
                error!("[IndexingServiceImpl::process_spent_type_full] Failed to switch the alias to the new index. {:#}", e);
            })?;

        self.elastic_service
            .delete_indices(&old_indexies)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::process_spent_type_full] delete_indices failed: {:#}",
                    e
                );
            })?;
        
        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_type_full] Completed. total_indexed={}",
            total_indexed
        );

        Ok(())
    }
}

// ============================================================================
// Trait impl
// ============================================================================

#[async_trait]
impl<M, P, E, C> IndexingService for IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    /// Migrates all `SpentDetail` records from MySQL to a Kafka topic.
    ///
    /// Delegates to [`migrate_to_kafka`]. Purges the target topic first,
    /// then produces all MySQL records as JSON messages.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (topic name, batch size, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if topic purge fails or Kafka produce fails.
    async fn run_spent_detail_migration_to_kafka(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        self.migrate_to_kafka(schedule_item).await
    }

    /// Performs a zero-downtime full reindex using a Blue/Green strategy.
    ///
    /// Orchestrates the complete pipeline from MySQL to Elasticsearch:
    ///
    /// # Steps
    ///
    /// 1. **Migration** — Purge and repopulate the full Kafka topic from MySQL (`migrate_to_kafka`)
    /// 2. **New Index** — Create a new Elasticsearch index with bulk-optimized settings
    /// 3. **Offset Snapshot** — Replicate the incremental consumer group offset to capture
    ///    the starting point for the catch-up phase
    /// 4. **Static Indexing** — Consume all messages from the full topic into the new index
    /// 5. **Dynamic Catch-up** — Consume incremental messages produced during static indexing,
    ///    pause the live incremental indexer when lag is small, and swap aliases once lag reaches 0
    /// 6. **Resume** — Re-enable the live incremental indexer on the new index
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (index name, topics, consumer groups,
    ///   batch size, mapping schema, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - MySQL migration fails
    /// - Elasticsearch index creation fails
    /// - Static or dynamic indexing fails
    /// - Alias swap fails
    async fn run_spent_detail_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        let index_name: &str = schedule_item.index_name();
        let incre_topic_name: &str = schedule_item.relation_topic_sub();
        let incre_source_group: &str = schedule_item.consumer_group_sub();
        let incre_target_group: &str = schedule_item.consumer_group();

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Starting full indexing for '{}'",
            index_name
        );

        // Step 0: 기존 alias 에 지정된 index 이름을 가져와준다.
        let old_indxies: Vec<String> = self
            .elastic_service
            .get_index_name_by_alias(index_name)
            .await?;

        // Step 1: migration
        self.migrate_to_kafka(schedule_item)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] migration failed: {:#}",
                    e
                );
            })?;

        // Step 2: create new index
        let new_index_name: String = self
            .elastic_service
            .prepare_full_index(index_name, schedule_item.mapping_schema())
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] prepare_full_index failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Created new index: {}",
            new_index_name
        );

        // Step 3: snapshot incremental offset
        match self
            .consume_service
            .replicate_consumer_group_offsets(
                incre_topic_name,
                incre_source_group,
                incre_target_group,
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] replicate offsets: {:#}",
                    e
                );
            }
        }

        // Step 4: static indexing
        let static_indexed: u64 = self
            .process_spent_detail_static(schedule_item, &new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] static phase failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Static indexing done: {} docs",
            static_indexed
        );

        // Step 5: dynamic catch-up + alias swap
        let dynamic_indexed: u64 = self
            .process_spent_detail_dynamic(schedule_item, &new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] dynamic phase failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Dynamic catch-up done: {} docs",
            dynamic_indexed
        );

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Total indexed: {} ({} static + {} dynamic)",
            static_indexed + dynamic_indexed,
            static_indexed,
            dynamic_indexed
        );

        // Step 6. Remove all indexes that were previously assigned to this alias.
        self.elastic_service
            .delete_indices(&old_indxies)
            .await
            .inspect_err(|e| error!("[IndexingServiceImpl::run_spent_detail_full] {:#}", e))?;

        Ok(())
    }

    /// Performs continuous incremental indexing into the write alias.
    ///
    /// Runs as an infinite loop, consuming messages from `relation_topic` and applying
    /// insert / update / delete operations to `write_{index_alias}`. This function is
    /// intentionally long-running and only exits early if an Elasticsearch bulk operation fails.
    ///
    /// # Global State Coordination
    ///
    /// Before consuming each batch, checks `get_spent_detail_indexing()`. If the flag is
    /// `false` (set by the dynamic catch-up phase during a full reindex), the loop sleeps
    /// and retries until the flag is re-enabled. This prevents the incremental indexer from
    /// writing to the old index while the alias is being swapped.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (index alias, topic, consumer group,
    ///   batch size, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` only if an early exit occurs (unreachable in normal operation).
    ///
    /// # Errors
    ///
    /// Returns an error if any Elasticsearch bulk operation fails.
    async fn run_spent_detail_incremental(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let write_index_alias: String = format!("write_{}", index_alias);
        let relation_topic: &str = schedule_item.relation_topic();
        let batch_size: usize = *schedule_item.batch_size();
        let consumer_group: &str = schedule_item.consumer_group();

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_incremental] Starting. topic='{}', write_alias='{}'",
            relation_topic,
            write_index_alias
        );

        let mut total_processed: u64 = 0;

        loop {
            let indexing_check: bool = get_spent_detail_indexing().await;

            if !indexing_check {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let messages: Vec<SpentDetailWithRelations> = match self
                .consume_service
                .consume_messages_as_with_group(relation_topic, batch_size, consumer_group)
                .await
            {
                Ok(m) => m,
                Err(e) => {
                    batch_log!(
                        error,
                        "[IndexingServiceImpl::run_spent_detail_incremental] consume failed: {:#}",
                        e
                    );
                    continue;
                }
            };

            if messages.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::run_spent_detail_incremental] No messages, waiting..."
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let batch_count: usize = messages.len();

            batch_log!(
                info,
                "[IndexingServiceImpl::run_spent_detail_incremental] Consumed {} messages from '{}'",
                batch_count,
                relation_topic
            );

            let mut to_insert: Vec<SpentDetailWithRelationsEs> = Vec::new();
            let mut to_update: Vec<SpentDetailWithRelationsEs> = Vec::new();
            let mut to_delete: Vec<i64> = Vec::new();

            for msg in messages {
                match msg.indexing_type {
                    IndexingType::Insert => to_insert.push(msg.into()),
                    IndexingType::Update => to_update.push(msg.into()),
                    IndexingType::Delete => to_delete.push(msg.spent_idx),
                }
            }

            if !to_insert.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::run_spent_detail_incremental] Inserting {} docs into '{}'",
                    to_insert.len(),
                    write_index_alias
                );
                self.elastic_service
                    .bulk_index(&write_index_alias, to_insert)
                    .await
                    .inspect_err(|e| {
                        error!("[IndexingServiceImpl::run_spent_detail_incremental] bulk_index failed: {:#}", e);
                    })?;
            }

            if !to_update.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::run_spent_detail_incremental] Updating {} docs in '{}'",
                    to_update.len(),
                    write_index_alias
                );
                self.elastic_service
                    .bulk_update(&write_index_alias, to_update, "spent_idx")
                    .await
                    .inspect_err(|e| {
                        error!("[IndexingServiceImpl::run_spent_detail_incremental] bulk_update failed: {:#}", e);
                    })?;
            }

            if !to_delete.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::run_spent_detail_incremental] Deleting {} docs from '{}'",
                    to_delete.len(),
                    write_index_alias
                );
                self.elastic_service
                    .bulk_delete(&write_index_alias, to_delete)
                    .await
                    .inspect_err(|e| {
                        error!("[IndexingServiceImpl::run_spent_detail_incremental] bulk_delete failed: {:#}", e);
                    })?;
            }

            total_processed += batch_count as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::run_spent_detail_incremental] Processed {} messages (total: {})",
                batch_count,
                total_processed
            );

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Performs full indexing of spent type keywords from MySQL into Elasticsearch.
    ///
    /// Delegates to the private [`process_spent_type_full`] helper.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (index name, batch size, mapping schema, etc.)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Index creation or preparation fails
    /// - MySQL batch fetch fails
    /// - Elasticsearch bulk indexing fails
    /// - Index finalization or alias swap fails
    /// - Old index deletion fails
    async fn run_spent_type_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        self.process_spent_type_full(schedule_item).await
    }
}
