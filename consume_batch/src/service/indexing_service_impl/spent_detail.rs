use crate::batch_log;
use crate::common::*;
use crate::enums::IndexingType;
use crate::global_state::*;
use crate::models::{
    SpentDetailFromKafka, SpentDetailIndexing, SpentDetailWithRelations,
    batch_schedule::BatchScheduleItem, ConsumerGroupLag
};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService, mysql_service::MysqlService,
    producer_service::ProducerService,
};

use super::IndexingServiceImpl;

fn merge_events_to_action_ids(
    messages: Vec<SpentDetailFromKafka>,
) -> anyhow::Result<(Vec<i64>, Vec<i64>)> {

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
        match msg.to_indexing_type()? {
            IndexingType::Insert | IndexingType::Update => upsert_ids.push(id),
            IndexingType::Delete => delete_ids.push(id),
        }
    }

    Ok((upsert_ids, delete_ids))
}

impl<M, P, E, C> IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    async fn input_spent_detail_full_data(
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
                .find_spent_detail_indexing_for_index(offset, batch_size as u64)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_full_data] Failed to load from DB: {:#}",
                        e
                    );
                })?;

            if rows.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::input_spent_detail_full_data] No more rows in DB, finishing"
                );
                break;
            }

            let batch_count: usize = rows.len();

            self.elastic_service
                .input_bulk(new_index_name, rows, Some("spent_idx"))
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_full_data] bulk_index failed: {:#}",
                        e
                    );
                })?;

            offset += batch_count as u64;
            total_indexed += batch_count as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_full_data] Indexed {} docs so far",
                total_indexed
            );
        }
        
        Ok(total_indexed)
    }

    async fn input_spent_detail_incremental_data(
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
            .find_messages_as_by_group(indexer_topic, batch_size, consumer_group)
            .await
            .inspect_err(|e| {
                batch_log!(
                    error,
                    "[IndexingServiceImpl::input_spent_detail_incremental_data] consume failed: {:#}",
                    e
                );
            })?;

        if messages.is_empty() {
            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_incremental_data] No messages, waiting..."
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        let batch_count: usize = messages.len();

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_incremental_data] Consumed {} messages from '{}'",
            batch_count,
            indexer_topic
        );

        let (upsert_ids, delete_ids) = merge_events_to_action_ids(messages)?;

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_incremental] After merge (reg_at-based): raw={}, upsert={}, delete={}",
            batch_count,
            upsert_ids.len(),
            delete_ids.len()
        );

        if !upsert_ids.is_empty() {
            let upsert_ids_size: u64 = upsert_ids.len() as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_incremental] Upsert {} docs from '{}'",
                upsert_ids_size,
                target_index_name
            );

            let upsert_list: Vec<SpentDetailWithRelations> = self
                .mysql_service
                .find_spent_details_for_indexing(&upsert_ids)
                .await?;

            self.mysql_service
                .modify_spent_detail_indexing(upsert_list)
                .await?;

            let to_es_upsert_list: Vec<SpentDetailIndexing> = self
                .mysql_service
                .find_spent_detail_indexing_by_ids(&upsert_ids)
                .await?;

            self.elastic_service
                .input_bulk(target_index_name, to_es_upsert_list, Some("spent_idx"))
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_incremental] bulk_index failed: {:#}",
                        e
                    );
                })?;

            upsert_processed += upsert_ids_size;
        }

        if !delete_ids.is_empty() {
            let delete_ids_size: u64 = delete_ids.len() as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_incremental] Deleting {} docs from '{}'",
                delete_ids.len(),
                target_index_name
            );

            self.mysql_service
                .delete_spent_detail_indexing(&delete_ids)
                .await?;

            self.elastic_service
                .delete_bulk(target_index_name, delete_ids)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_incremental] bulk_delete failed: {:#}",
                        e
                    );
                })?;

            delete_processed += delete_ids_size;
        }

        Ok((upsert_processed, delete_processed))
    }

    async fn input_spent_detail_catch_up(
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
            "[IndexingServiceImpl::input_spent_detail_catch_up] Starting. topic='{}', index='{}', ref='{}', catchup='{}'",
            relation_topic,
            index_name,
            consumer_group,
            consumer_group_sub
        );

        const MAX_CONSECUTIVE_ERRORS: u32 = 5;

        let mut total_upsert_processed: u64 = 0;
        let mut total_delete_processed: u64 = 0;
        let mut indexing_paused: bool = false;
        let mut consecutive_errors: u32 = 0;

        loop {
            
            let lag_info: ConsumerGroupLag = self
                .consume_service
                .find_consumer_group_lag_by_partition(
                    relation_topic,
                    consumer_group,
                    consumer_group_sub,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_catch_up] Failed to get lag: {:#}",
                        e
                    );
                })?;

            let lag: i64 = lag_info.total_lag;

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_catch_up] lag={}, batch_size={}, partitions={}",
                lag,
                batch_size,
                lag_info.partition_lags.len()
            );

            for partition_lag in &lag_info.partition_lags {
                if partition_lag.lag > 0 {
                    info!(
                        "[IndexingServiceImpl::input_spent_detail_catch_up] Partition {}: lag={} (ref={}, catchup={})",
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
                    "[IndexingServiceImpl::input_spent_detail_catch_up] Almost caught up (lag={}). Pausing incremental indexing.",
                    lag
                );
                set_spent_detail_indexing(false).await;
                indexing_paused = true;
            }

            if indexing_paused && lag == 0 {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::input_spent_detail_catch_up] Fully caught up. total_processed={}. Swapping aliases.",
                    total_upsert_processed + total_delete_processed
                );

                self.elastic_service
                    .finalize_index_settings(index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::input_spent_detail_catch_up] Failed to revert the index settings.{:#}",
                            e
                        );
                    })?;
                
                self.elastic_service
                    .modify_write_alias(&write_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::input_spent_detail_catch_up] update_write_alias failed: {:#}",
                            e
                        );
                    })?;

                self.elastic_service
                    .modify_read_alias(&read_alias, index_name)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[IndexingServiceImpl::input_spent_detail_catch_up] update_read_alias failed: {:#}",
                            e
                        );
                    })?;

                batch_log!(
                    info,
                    "[IndexingServiceImpl::input_spent_detail_catch_up] Alias swap complete. Resuming incremental indexing."
                );

                set_spent_detail_indexing(true).await;
                break;
            }

            let (upsert_processed, delete_processed) = match self
                .input_spent_detail_incremental_data(
                    relation_topic,
                    consumer_group,
                    index_name,
                    batch_size,
                )
                .await
            {
                Ok((upsert_processed, delete_processed)) => {
                    consecutive_errors = 0;
                    (upsert_processed, delete_processed)
                }
                Err(e) => {
                    consecutive_errors += 1;
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_catch_up] error ({}/{}): {:#}",
                        consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                    );
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        return Err(anyhow!(
                            "[IndexingServiceImpl::input_spent_detail_catch_up] Aborting catch-up after {} consecutive errors. Last error: {:#}",
                            MAX_CONSECUTIVE_ERRORS,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_catch_up] upsert: {}, delete: {} (total: {})",
                upsert_processed,
                delete_processed,
                upsert_processed + delete_processed
            );

            total_upsert_processed += upsert_processed;
            total_delete_processed += delete_processed;
        }

        Ok(total_upsert_processed + total_delete_processed)
    }

    pub(super) async fn input_spent_detail_full(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let incre_topic_name: &str = schedule_item.relation_topic_sub();
        let incre_source_group: &str = schedule_item.consumer_group_sub();
        let incre_target_group: &str = schedule_item.consumer_group();

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_full] Starting full indexing for '{}'",
            index_alias
        );

        let old_indxies: Vec<String> = self
            .elastic_service
            .find_index_name_by_alias(index_alias)
            .await?;

        let new_index_name: String = self
            .elastic_service
            .initialize_full_index(index_alias, schedule_item.mapping_schema())
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_detail_full] prepare_full_index failed: {:#}",
                    e
                );
            })?;
        
        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_full] Created new index: {}",
            new_index_name
        );

        match self
            .consume_service
            .modify_consumer_group_offsets(incre_topic_name, incre_source_group, incre_target_group)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "[IndexingServiceImpl::input_spent_detail_full] replicate offsets: {:#}",
                    e
                );
            }
        }


        let full_indexed: u64 = match self
            .input_spent_detail_full_data(schedule_item, &new_index_name)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                error!(
                    "[IndexingServiceImpl::input_spent_detail_full] static phase failed: {:#}",
                    e
                );
                self.delete_orphaned_index(&new_index_name).await;
                return Err(e);
            }
        };

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_full] Full indexing done: {} docs",
            full_indexed
        );

        let catch_up_indexed: u64 = match self
            .input_spent_detail_catch_up(schedule_item, &new_index_name)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                error!(
                    "[IndexingServiceImpl::input_spent_detail_full] incremental phase failed: {:#}",
                    e
                );
                self.delete_orphaned_index(&new_index_name).await;
                return Err(e);
            }
        };

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_full] incremental catch-up done: {} docs",
            catch_up_indexed
        );

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_full] Total indexed: {} ({} full + {} incremental)",
            full_indexed + catch_up_indexed,
            full_indexed,
            catch_up_indexed
        );

        if let Err(e) = self
            .elastic_service
            .finalize_index_settings(&new_index_name)
            .await
        {
            error!(
                "[IndexingServiceImpl::input_spent_detail_full] Failed to revert the index settings.: {:#}",
                e
            );
            self.delete_orphaned_index(&new_index_name).await;
            return Err(e);
        }

        if let Err(e) = self
            .elastic_service
            .modify_alias(index_alias, &new_index_name)
            .await
        {
            error!(
                "[IndexingServiceImpl::input_spent_detail_full] Failed to switch the alias to the new index. {:#}",
                e
            );
            self.delete_orphaned_index(&new_index_name).await;
            return Err(e);
        }

        self.elastic_service
            .delete_indices(&old_indxies)
            .await
            .inspect_err(|e| error!("[IndexingServiceImpl::input_spent_detail_full] {:#}", e))?;

        Ok(())
    }

    pub(super) async fn input_spent_detail_incremental(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let write_index_alias: String = format!("write_{}", index_alias);
        let relation_topic: &str = schedule_item.relation_topic();
        let batch_size: usize = *schedule_item.batch_size();
        let consumer_group: &str = schedule_item.consumer_group(); // incremental_spent_detail_group_dev

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_detail_incremental] Starting. topic='{}', write_alias='{}'",
            relation_topic,
            write_index_alias
        );

        const MAX_CONSECUTIVE_ERRORS: u32 = 5;
        let mut consecutive_errors: u32 = 0;

        loop {
            let indexing_check: bool = get_spent_detail_indexing().await;

            if !indexing_check {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let (upsert_processed, delete_processed) = match self
                .input_spent_detail_incremental_data(
                    relation_topic,
                    consumer_group,
                    &write_index_alias,
                    batch_size,
                )
                .await
            {
                Ok((upsert_processed, delete_processed)) => {
                    consecutive_errors = 0;
                    (upsert_processed, delete_processed)
                }
                Err(e) => {
                    consecutive_errors += 1;
                    error!(
                        "[IndexingServiceImpl::input_spent_detail_incremental] error ({}/{}): {:#}",
                        consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                    );
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        return Err(anyhow!(
                            "[IndexingServiceImpl::input_spent_detail_incremental] Aborting incremental indexing after {} consecutive errors. Last error: {:#}",
                            MAX_CONSECUTIVE_ERRORS,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_detail_incremental] upsert: {}, delete: {} (total: {})",
                upsert_processed,
                delete_processed,
                upsert_processed + delete_processed
            );

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
