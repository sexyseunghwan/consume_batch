//! Bulk re-classify `consume_keyword_type` columns in SPENT_DETAIL and SPENT_DETAIL_INDEXING.

use crate::models::{ConsumingIndexProdtType, SpentDetail, SpentDetailIndexing, batch_schedule::*};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

use super::BatchServiceImpl;

impl<M, E, C, P, D, I, S> BatchServiceImpl<M, E, C, P, D, I, S>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
{
    /// Runs `modify_all_spent_detail_type` and `modify_all_spent_detail_indexing_type` in sequence.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (batch size, index name, etc.)
    /// * `mysql_service` - MySQL service for fetching and updating records
    /// * `elastic_service` - Elasticsearch service for keyword type classification
    ///
    /// # Errors
    ///
    /// Returns an error if either inner function fails.
    pub(super) async fn modify_all_spent_detail_types(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        Self::modify_all_spent_detail_type(schedule_item, mysql_service, elastic_service).await?;
        Self::modify_all_spent_detail_indexing_type(schedule_item, mysql_service, elastic_service)
            .await?;
        Ok(())
    }

    /// Re-evaluates and updates the `consume_keyword_type_id` for all spent details.
    ///
    /// Fetches all records from MySQL in batches, queries Elasticsearch to determine
    /// the correct keyword type for each record's `spent_name`, and bulk-updates
    /// any records whose type has changed.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (batch size, index name, etc.)
    /// * `mysql_service` - MySQL service for fetching and updating spent details
    /// * `elastic_service` - Elasticsearch service for keyword type classification
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - MySQL fetch or batch update fails
    /// - Elasticsearch type judgement query fails
    async fn modify_all_spent_detail_type(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;

        batch_log!(
            info,
            "[BatchServiceImpl::modify_all_spent_detail_type] Starting. batch_size={}",
            batch_size
        );

        const MAX_RETRIES: u32 = 3;

        let mut offset: u64 = 0;
        let mut total_updated: u64 = 0;
        let mut total_processed: u64 = 0;

        loop {
            /*
                details = [
                    { spent_idx: 152, spent_name: "네이버페이", ... }, -> D1
                    { spent_idx: 153, spent_name: "쿠팡",       ... }, -> D2
                    { spent_idx: 157, spent_name: "카카오",     ... }  -> D3
                ]
            */
            let details: Vec<SpentDetail> = {
                let mut last_err: Option<anyhow::Error> = None;
                let mut result: Option<Vec<SpentDetail>> = None;

                for attempt in 1..=MAX_RETRIES {
                    match mysql_service.find_spent_details(offset, batch_size).await {
                        Ok(rows) => {
                            result = Some(rows);
                            break;
                        }
                        Err(e) => {
                            let delay_secs: u64 = 2u64.pow(attempt - 1);
                            batch_log!(
                                error,
                                "[BatchServiceImpl::modify_all_spent_detail_type] fetch failed (attempt {}/{}, offset={}, retry in {}s): {:#}",
                                attempt,
                                MAX_RETRIES,
                                offset,
                                delay_secs,
                                e
                            );
                            last_err = Some(e);
                            if attempt < MAX_RETRIES {
                                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                            }
                        }
                    }
                }

                match result {
                    Some(rows) => rows,
                    None => {
                        batch_log!(
                            error,
                            "[BatchServiceImpl::modify_all_spent_detail_type] Aborting: all {} retries exhausted at offset={}. Last error: {:#}",
                            MAX_RETRIES,
                            offset,
                            last_err.unwrap()
                        );
                        break;
                    }
                }
            };

            if details.is_empty() {
                break;
            }

            let batch_count: usize = details.len();
            total_processed += batch_count as u64;

            let spent_names: Vec<String> = details.iter().map(|d| d.spent_name().clone()).collect();

            /*
                spent_types = [
                    { consume_keyword: "네이버페이", consume_keyword_type: "인터넷 쇼핑", consume_keyword_type_id: 16, ... },
                    { consume_keyword: "쿠팡",       consume_keyword_type: "인터넷 쇼핑", consume_keyword_type_id: 16, ... },
                    { consume_keyword: "카카오",     consume_keyword_type: "인터넷 쇼핑", consume_keyword_type_id: 16, ... },
                ]
            */
            let spent_types: Vec<ConsumingIndexProdtType> = elastic_service
                .find_consume_type_judgements(&spent_names)
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::modify_all_spent_detail_type] spent_types: {:#}",
                        e
                    );
                })?;

            if spent_types.len() != details.len() {
                return Err(anyhow!(
                    "[BatchServiceImpl::modify_all_spent_detail_type] spent_types length mismatch. details={}, spent_types={}",
                    details.len(),
                    spent_types.len()
                ));
            }

            /*
                updates = [
                    (D1.spent_idx, S1.consume_keyword_type_id),
                    (D2.spent_idx, S2.consume_keyword_type_id),
                    (D3.spent_idx, S3.consume_keyword_type_id),
                ]
            */
            let updates: Vec<(i64, i64)> = details
                .iter()
                .zip(spent_types.iter())
                .map(|(detail, spent_type)| {
                    (*detail.spent_idx(), *spent_type.consume_keyword_type_id())
                })
                .collect();

            if !updates.is_empty() {
                let update_count: usize = updates.len();
                let updated: u64 = mysql_service
                    .modify_spent_detail_type_batch(updates, batch_size as usize)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[modify_all_spent_detail_type] Failed to update batch: {:#}",
                            e
                        );
                    })?;

                total_updated += updated;

                batch_log!(
                    info,
                    "[modify_all_spent_detail_type] Batch offset={}, updated {}/{} records",
                    offset,
                    update_count,
                    batch_count
                );
            }

            offset += batch_size;
        }

        batch_log!(
            info,
            "[modify_all_spent_detail_type] Completed. processed={}, updated={}",
            total_processed,
            total_updated
        );

        Ok(())
    }

    /// Re-evaluates and updates the `consume_keyword_type_id` and `consume_keyword_type`
    /// for all spent detail indexing records.
    ///
    /// Fetches all records from `SPENT_DETAIL_INDEXING` in batches, queries Elasticsearch to
    /// determine the correct keyword type for each record's `spent_name`, and bulk-updates
    /// any records whose type has changed.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule configuration (batch size, index name, etc.)
    /// * `mysql_service` - MySQL service for fetching and updating spent detail indexing records
    /// * `elastic_service` - Elasticsearch service for keyword type classification
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - MySQL fetch or batch update fails
    /// - Elasticsearch type judgement query fails
    async fn modify_all_spent_detail_indexing_type(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;

        batch_log!(
            info,
            "[BatchServiceImpl::modify_all_spent_detail_indexing_type] Starting. batch_size={}",
            batch_size
        );

        const MAX_RETRIES: u32 = 3;

        let mut offset: u64 = 0;
        let mut total_updated: u64 = 0;
        let mut total_processed: u64 = 0;

        loop {
            let details: Vec<SpentDetailIndexing> = {
                let mut last_err: Option<anyhow::Error> = None;
                let mut result: Option<Vec<SpentDetailIndexing>> = None;

                for attempt in 1..=MAX_RETRIES {
                    match mysql_service
                        .find_spent_detail_indexing_for_index(offset, batch_size)
                        .await
                    {
                        Ok(rows) => {
                            result = Some(rows);
                            break;
                        }
                        Err(e) => {
                            let delay_secs: u64 = 2u64.pow(attempt - 1);
                            batch_log!(
                                error,
                                "[BatchServiceImpl::modify_all_spent_detail_indexing_type] fetch failed (attempt {}/{}, offset={}, retry in {}s): {:#}",
                                attempt,
                                MAX_RETRIES,
                                offset,
                                delay_secs,
                                e
                            );
                            last_err = Some(e);
                            if attempt < MAX_RETRIES {
                                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                            }
                        }
                    }
                }

                match result {
                    Some(rows) => rows,
                    None => {
                        batch_log!(
                            error,
                            "[BatchServiceImpl::modify_all_spent_detail_indexing_type] Aborting: all {} retries exhausted at offset={}. Last error: {:#}",
                            MAX_RETRIES,
                            offset,
                            last_err.unwrap()
                        );
                        break;
                    }
                }
            };

            if details.is_empty() {
                break;
            }

            let batch_count: usize = details.len();
            total_processed += batch_count as u64;

            let spent_names: Vec<String> = details.iter().map(|d| d.spent_name().clone()).collect();

            let spent_types: Vec<ConsumingIndexProdtType> = elastic_service
                .find_consume_type_judgements(&spent_names)
                .await
                .inspect_err(|e| {
                    error!("[BatchServiceImpl::modify_all_spent_detail_indexing_type] spent_types: {:#}", e);
                })?;

            if spent_types.len() != details.len() {
                return Err(anyhow!(
                    "[BatchServiceImpl::modify_all_spent_detail_indexing_type] spent_types length mismatch. details={}, spent_types={}",
                    details.len(),
                    spent_types.len()
                ));
            }

            let updates: Vec<(i64, i64, String)> = details
                .iter()
                .zip(spent_types.iter())
                .map(|(detail, spent_type)| {
                    (
                        *detail.spent_idx(),
                        *spent_type.consume_keyword_type_id(),
                        spent_type.consume_keyword_type().clone(),
                    )
                })
                .collect();

            if !updates.is_empty() {
                let update_count: usize = updates.len();
                let updated: u64 = mysql_service
                    .modify_spent_detail_indexing_type_batch(updates, batch_size as usize)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[modify_all_spent_detail_indexing_type] Failed to update batch: {:#}",
                            e
                        );
                    })?;

                total_updated += updated;

                batch_log!(
                    info,
                    "[modify_all_spent_detail_indexing_type] Batch offset={}, updated {}/{} records",
                    offset,
                    update_count,
                    batch_count
                );
            }

            offset += batch_size;
        }

        batch_log!(
            info,
            "[modify_all_spent_detail_indexing_type] Completed. processed={}, updated={}",
            total_processed,
            total_updated
        );

        Ok(())
    }
}
