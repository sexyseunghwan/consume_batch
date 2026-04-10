use crate::batch_log;
use crate::common::*;
use crate::enums::IndexingType;
use crate::global_state::*;
use crate::models::{
    ConsumerGroupLag, SpentDetailFromKafka, SpentDetailIndexing, SpentDetailWithRelations,
    SpentDetailWithRelationsEs, SpentTypeKeyword, batch_schedule::BatchScheduleItem,
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

    async fn process_spent_detail_full(
        &self,
        schedule_item: &BatchScheduleItem,
        new_index_name: &str,
    ) -> anyhow::Result<u64> {
        let batch_size: usize = *schedule_item.batch_size();

        let mut offset: u64 = 0;
        let mut total_indexed: u64 = 0;

        loop {
            let rows: Vec<SpentDetailIndexing> = self
                .mysql_service
                .fetch_spent_detail_indexing_for_index(offset, batch_size as u64)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_static] Failed to load from DB: {:#}",
                        e
                    );
                })?;

            if rows.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_static] No more rows in DB, finishing"
                );
                break;
            }

            let batch_count: usize = rows.len();

            self.elastic_service
                .bulk_index(new_index_name, rows)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_static] bulk_index failed: {:#}",
                        e
                    );
                })?;

            offset += batch_count as u64;
            total_indexed += batch_count as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_static] Indexed {} docs so far",
                total_indexed
            );
        }

        Ok(total_indexed)
    }

    
    async fn process_spent_detail_incremental(
        &self,
        indexer_topic: &str,
        consumer_group: &str,
        target_index_name: &str,
        batch_size: usize,
    ) -> anyhow::Result<(u64, u64)> {

        let mut upsert_processed: u64 = 0;
        let mut delete_processed: u64 = 0;
        
        let messages: Vec<SpentDetailFromKafka> = self
            .consume_service
            .consume_messages_as_with_group(indexer_topic, batch_size, consumer_group)
            .await
            .inspect_err(|e| {
                batch_log!(
                    error,
                    "[IndexingServiceImpl::process_spent_detail_incremental] consume failed: {:#}",
                    e
                );
            })?;
        
        if messages.is_empty() {
            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_incremental] No messages, waiting..."
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let batch_count: usize = messages.len();

        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_detail_incremental] Consumed {} messages from '{}'",
            batch_count,
            indexer_topic
        );

        // Batch 내 동일 ID 이벤트를 압축해 최종 upsert / delete ID 목록을 결정한다.
        let (upsert_ids, delete_ids) = Self::merge_batch_events(messages)?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_incremental] After merge (reg_at-based): raw={}, upsert={}, delete={}",
            batch_count,
            upsert_ids.len(),
            delete_ids.len()
        );

        // Upsert : ES 및 역정규화 테이블에서 해당 문서 추가
        if !upsert_ids.is_empty() {
            let upsert_ids_size: u64 = upsert_ids.len() as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::run_spent_detail_incremental] Upsert {} docs from '{}'",
                upsert_ids_size,
                target_index_name
            );

            let upsert_list: Vec<SpentDetailWithRelations> = self
                .mysql_service
                .fetch_spent_details_for_indexing(&upsert_ids)
                .await?;

            // 역정규화 테이블에 데이터 insert 또는 update
            self.mysql_service
                .upsert_spent_detail_indexing(upsert_list)
                .await?;

            let to_es_upsert_list: Vec<SpentDetailIndexing> = self
                .mysql_service
                .fetch_spent_detail_indexing_by_ids(&upsert_ids)
                .await?;

            self.elastic_service
                .bulk_index(&target_index_name, to_es_upsert_list)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::run_spent_detail_incremental] bulk_index failed: {:#}",
                        e
                    );
                })?;

            upsert_processed += upsert_ids_size;
        }

        // Delete: ES 및 역정규화 테이블에서 해당 문서 삭제
        if !delete_ids.is_empty() {
            let delete_ids_size: u64 = delete_ids.len() as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::run_spent_detail_incremental] Deleting {} docs from '{}'",
                delete_ids.len(),
                target_index_name
            );

            self.mysql_service
                .delete_spent_detail_indexing(&delete_ids)
                .await?;

            self.elastic_service
                .bulk_delete(&target_index_name, delete_ids)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::run_spent_detail_incremental] bulk_delete failed: {:#}",
                        e
                    );
                })?;

            delete_processed += delete_ids_size;
        }

        Ok((upsert_processed, delete_processed))
    }

    async fn process_spent_detail_catch_up(
        &self,
        schedule_item: &BatchScheduleItem,
        index_name: &str,
    ) -> anyhow::Result<u64> {
        let relation_topic: &str = schedule_item.relation_topic_sub(); // 증분 인덱서 토픽 이름
        let batch_size: usize = *schedule_item.batch_size();
        let consumer_group: &str = schedule_item.consumer_group(); // full 인덱서 컨슈머 그룹
        let consumer_group_sub: &str = schedule_item.consumer_group_sub(); // 증분 인덱서 컨슈머 그룹
        let base_alias: &str = schedule_item.index_name();
        let write_alias: String = format!("write_{}", base_alias);
        let read_alias: String = format!("read_{}", base_alias);

        batch_log!(
            info,
            "[IndexingServiceImpl::process_spent_detail_catch_up] Starting. topic='{}', index='{}', ref='{}', catchup='{}'",
            relation_topic,
            index_name,
            consumer_group,
            consumer_group_sub
        );

        let mut total_upsert_processed: u64 = 0;
        let mut total_delete_processed: u64 = 0;
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
                        "[IndexingServiceImpl::process_spent_detail_catch_up] Failed to get lag: {:#}",
                        e
                    );
                })?;

            let lag: i64 = lag_info.total_lag;

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_catch_up] lag={}, batch_size={}, partitions={}",
                lag,
                batch_size,
                lag_info.partition_lags.len()
            );

            for partition_lag in &lag_info.partition_lags {
                if partition_lag.lag > 0 {
                    info!(
                        "[IndexingServiceImpl::process_spent_detail_catch_up] Partition {}: lag={} (ref={}, catchup={})",
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
                    "[IndexingServiceImpl::process_spent_detail_catch_up] Almost caught up (lag={}). Pausing incremental indexing.",
                    lag
                );

                // 현재 실시간으로 동작하고 있는 증분색인을 잠시 멈춰준다.
                set_spent_detail_indexing(false).await;
                indexing_paused = true;
            }

            if indexing_paused && lag == 0 {
                
                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_catch_up] Fully caught up. total_processed={}. Swapping aliases.",
                    total_upsert_processed + total_delete_processed
                );

                // 현재 인덱스 설정정보 원복해준다 -> refresh, replication
                self.elastic_service
                    .revert_index_setting(index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_catch_up] Failed to revert the index settings.{:#}",
                            e
                        );
                    })?;

                self.elastic_service
                    .update_write_alias(&write_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_catch_up] update_write_alias failed: {:#}",
                            e
                        );
                    })?;

                self.elastic_service
                    .update_read_alias(&read_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::process_spent_detail_catch_up] update_read_alias failed: {:#}",
                            e
                        );
                    })?;

                batch_log!(
                    info,
                    "[IndexingServiceImpl::process_spent_detail_catch_up] Alias swap complete. Resuming incremental indexing."
                );

                // 실시간 증분색인 재개
                set_spent_detail_indexing(true).await;
                // consumer-lag 가 더이상 없으므로 loop 종료
                break;
            }


            // 증분 catch-up 색인 진행
            let (upsert_processed, delete_processed) = match self
                .process_spent_detail_incremental(
                    relation_topic,
                    consumer_group,
                    &write_alias,
                    batch_size,
                )
                .await
            {
                Ok((upsert_processed, delete_processed)) => (upsert_processed, delete_processed),
                Err(e) => {
                    error!(
                        "[IndexingServiceImpl::process_spent_detail_catch_up] {:#}",
                        e
                    );
                    continue;
                }
            };
            

            batch_log!(
                info,
                "[IndexingServiceImpl::process_spent_detail_catch_up] upsert: {}, delete: {} (total: {})",
                upsert_processed,
                delete_processed,
                upsert_processed + delete_processed
            );

            total_upsert_processed += upsert_processed;
            total_delete_processed += delete_processed;
        }

        Ok(total_upsert_processed + total_delete_processed)
    }

    fn merge_batch_events(
        messages: Vec<SpentDetailFromKafka>,
    ) -> anyhow::Result<(Vec<i64>, Vec<i64>)> {
        use std::collections::hash_map::Entry;

        // 동일 ID에 대해 reg_at이 가장 최신인 이벤트 하나만 남긴다.
        let mut latest: HashMap<i64, SpentDetailFromKafka> = HashMap::new();

        for msg in messages {
            match latest.entry(msg.spent_idx) {
                Entry::Vacant(e) => {
                    e.insert(msg);
                }
                Entry::Occupied(mut e) => {
                    if msg.reg_at > e.get().reg_at {
                        e.insert(msg);
                    }
                }
            }
        }

        let mut upsert_ids: Vec<i64> = Vec::new();
        let mut delete_ids: Vec<i64> = Vec::new();

        for (id, msg) in latest {
            match msg.convert_indexing_type()? {
                IndexingType::Insert | IndexingType::Update => upsert_ids.push(id),
                IndexingType::Delete => delete_ids.push(id),
            }
        }

        Ok((upsert_ids, delete_ids))
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
    
    
    
    
    async fn run_spent_detail_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        
        let index_alias: &str = schedule_item.index_name();
        let incre_topic_name: &str = schedule_item.relation_topic_sub();
        let incre_source_group: &str = schedule_item.consumer_group_sub();
        let incre_target_group: &str = schedule_item.consumer_group();

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Starting full indexing for '{}'",
            index_alias
        );

        // Step 1: 기존 alias 에 지정된 index 이름을 가져와준다.
        let old_indxies: Vec<String> = self
            .elastic_service
            .get_index_name_by_alias(index_alias)
            .await?;

        // Step 2: create new index
        let new_index_name: String = self
            .elastic_service
            .prepare_full_index(index_alias, schedule_item.mapping_schema())
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

        // Step 3: snapshot incremental offset - 이게 그렇게 좋아보이진 않는데...
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

        // Step 4: Full indexing
        let full_indexed: u64 = self
            .process_spent_detail_full(schedule_item, &new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] static phase failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Full indexing done: {} docs",
            full_indexed
        );

        // Step 5: dynamic catch-up
        let catch_up_indexed: u64 = self
            .process_spent_detail_catch_up(schedule_item, &new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] incremental phase failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] incremental catch-up done: {} docs",
            catch_up_indexed
        );

        batch_log!(
            info,
            "[IndexingServiceImpl::run_spent_detail_full] Total indexed: {} ({} full + {} incremental)",
            full_indexed + catch_up_indexed,
            full_indexed,
            catch_up_indexed
        );

        // Step 6. Revert index settings
        self.elastic_service
            .revert_index_setting(&new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::run_spent_detail_full] Failed to revert the index settings.: {:#}",
                    e
                )
            })?;

        // Step 7. Alias swap
        self.elastic_service
            .swap_alias(index_alias, &new_index_name)
            .await
            .inspect_err(|e| {
                error!("[IndexingServiceImpl::run_spent_detail_full] Failed to switch the alias to the new index. {:#}", e);
            })?;

        // Step 8. Remove all indexes that were previously assigned to this alias. -> Option
        self.elastic_service
            .delete_indices(&old_indxies)
            .await
            .inspect_err(|e| error!("[IndexingServiceImpl::run_spent_detail_full] {:#}", e))?;

        Ok(())
    }


    // 이쪽이 증분색인 !!!
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

        loop {
            
            let indexing_check: bool = get_spent_detail_indexing().await;

            if !indexing_check {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let (upsert_processed, delete_processed) = match self
                .process_spent_detail_incremental(
                    relation_topic,
                    consumer_group,
                    &write_index_alias,
                    batch_size,
                )
                .await
            {
                Ok((upsert_processed, delete_processed)) => (upsert_processed, delete_processed),
                Err(e) => {
                    error!(
                        "[IndexingServiceImpl::run_spent_detail_incremental] {:#}",
                        e
                    );
                    continue;
                }
            };

            batch_log!(
                info,
                "[IndexingServiceImpl::run_spent_detail_incremental] upsert: {}, delete: {} (total: {})",
                upsert_processed,
                delete_processed,
                upsert_processed + delete_processed
            );

            tokio::time::sleep(Duration::from_millis(500)).await;
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
