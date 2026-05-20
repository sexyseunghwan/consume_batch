use crate::batch_log;
use crate::common::*;
use crate::models::{SpentTypeKeyword, batch_schedule::BatchScheduleItem};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService, mysql_service::MysqlService,
    producer_service::ProducerService,
};

use super::IndexingServiceImpl;

impl<M, P, E, C> IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    /// Fetches all SpentType keywords from MySQL in batches and bulk-indexes them
    /// into a new Elasticsearch index, then finalizes settings and swaps the alias.
    async fn input_spent_type_full_data(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let batch_size: usize = *schedule_item.batch_size();

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_type_full_data] Processing {} (index: {})",
            schedule_item.batch_name(),
            index_alias
        );

        let old_indexies: Vec<String> = self
            .elastic_service
            .find_index_name_by_alias(index_alias)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_type_full_data] Failed to find any existing indices associated with the specified alias: {:#}",
                    e
                )
            })?;

        let new_index_name: String = self
            .elastic_service
            .initialize_full_index(index_alias, schedule_item.mapping_schema())
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_type_full_data] prepare_full_index: {:#}",
                    e
                );
            })?;

        let mut offset: u64 = 0;
        let mut total_indexed: u64 = 0;

        loop {
            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_type_full_data] Fetching batch at offset={}, batch_size={}",
                offset,
                batch_size
            );

            let keywords: Vec<SpentTypeKeyword> = self
                .mysql_service
                .find_spent_type_keywords_batch(offset, batch_size as u64)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_type_full_data] Failed to fetch keywords: {:#}",
                        e
                    );
                })?;

            if keywords.is_empty() {
                batch_log!(
                    info,
                    "[IndexingServiceImpl::input_spent_type_full_data] No more data to index"
                );
                break;
            }

            let batch_count: usize = keywords.len();

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_type_full_data] Indexing {} documents",
                batch_count
            );

            self.elastic_service
                .input_bulk(&new_index_name, keywords, None)
                .await
                .inspect_err(|e| {
                    error!(
                        "[IndexingServiceImpl::input_spent_type_full_data] bulk_index failed: {:#}",
                        e
                    );
                })?;

            total_indexed += batch_count as u64;
            offset += batch_size as u64;

            batch_log!(
                info,
                "[IndexingServiceImpl::input_spent_type_full_data] Indexed {} documents so far",
                total_indexed
            );
        }

        self.elastic_service
            .modify_index_setting(&new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_type_full_data] Failed to revert the index settings.: {:#}",
                    e
                )
            })?;

        self.elastic_service
            .modify_alias(index_alias, &new_index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_type_full_data] Failed to switch the alias to the new index. {:#}",
                    e
                );
            })?;

        self.elastic_service
            .delete_indices(&old_indexies)
            .await
            .inspect_err(|e| {
                error!(
                    "[IndexingServiceImpl::input_spent_type_full_data] delete_indices failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[IndexingServiceImpl::input_spent_type_full_data] Completed. total_indexed={}",
            total_indexed
        );

        Ok(())
    }

    /// Performs full indexing of spent type keywords from MySQL into Elasticsearch.
    pub(super) async fn input_spent_type_full(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        self.input_spent_type_full_data(schedule_item).await
    }
}
