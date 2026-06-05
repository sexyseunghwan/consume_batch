//! Routes each `batch_name` to its concrete handler.

use crate::models::batch_schedule::*;
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    redis_service::RedisService, smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

use super::BatchServiceImpl;

impl<M, E, C, P, D, I, S, R> BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
    R: RedisService + Send + Sync + 'static,
{
    // 여기가 진짜 실행할 함수들의 모음이 존재함...
    pub(super) async fn execute_batch_by_name(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
        public_data_service: &Arc<D>,
        indexing_service: &Arc<I>,
        smtp_service: &Arc<S>,
        redis_service: &Arc<R>,
    ) -> Result<()> {
        let start_time: DateTime<Utc> = Utc::now();
        let batch_name: &str = schedule_item.batch_name();
        let index_name: &str = schedule_item.index_name();

        batch_log!(
            info,
            "[BatchServiceImpl::input_batch_by_schedule] {} Starting batch indexing job",
            index_name
        );

        match batch_name {
            "spent_detail_full" => {
                indexing_service
                    .input_spent_detail_full(schedule_item)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::input_batch_by_schedule] spent_detail_full: {:#}",
                            e
                        );
                    })?;
            }
            "spent_detail_incremental" => {
                indexing_service
                    .input_spent_detail_incremental(schedule_item)
                    .await
                    .inspect_err(|e| {
                        error!("[BatchServiceImpl::input_batch_by_schedule] spent_detail_incremental: {:#}", e);
                    })?;
            }
            "spent_type" => {
                indexing_service
                    .input_spent_type_full(schedule_item)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::input_batch_by_schedule] spent_type: {:#}",
                            e
                        );
                    })?;
            }
            "all_change_spent_detail_type" => {
                Self::modify_all_spent_detail_types(schedule_item, mysql_service, elastic_service)
                    .await
                    .inspect_err(|e| {
                        error!("[BatchServiceImpl::input_batch_by_schedule] all_change_spent_detail_type: {:#}", e);
                    })?;
            }
            "dimension_date_table" => {
                Self::input_date_dimension_data(schedule_item, mysql_service, public_data_service)
                    .await
                    .inspect_err(|e| {
                        error!("[BatchServiceImpl::input_batch_by_schedule] dimension_date_table: {:#}", e);
                    })?;
            }
            "monthly_spent_report" => {
                Self::send_monthly_spent_report(
                    schedule_item,
                    elastic_service,
                    mysql_service,
                    smtp_service,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] monthly_spent_report: {:#}",
                        e
                    );
                })?;
            }
            "weekly_spent_report" => {
                Self::send_weekly_spent_report(
                    schedule_item,
                    elastic_service,
                    mysql_service,
                    smtp_service,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] weekly_spent_report: {:#}",
                        e
                    );
                })?;
            }
            "sync_currency_exchange_rates" => {
                Self::sync_currency_exchange_rates(
                    mysql_service
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] sync_currency_exchange_rates: {:#}",
                        e
                    );
                })?
            }
            "sync_stock_price" => {
                Self::sync_stock_price(
                    schedule_item,
                    mysql_service,
                    redis_service,
                    elastic_service
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] sync_stock_price: {:#}",
                        e
                    );
                })?
            }
            "sync_crypto_price" => {
                Self::sync_crypto_price(
                    schedule_item,
                    mysql_service,
                    elastic_service
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] sync_crypto_price: {:#}",
                        e
                    );
                })?
            }
            "sync_current_asset_total" => {
                Self::sync_current_asset_total(
                    schedule_item,
                    mysql_service
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] sync_current_asset_total: {:#}",
                        e
                    );
                })?
            }
            _ => {
                batch_log!(
                    warn,
                    "[BatchServiceImpl::input_batch_by_schedule] Unknown batch_name: {}, skipping",
                    batch_name
                );
            }
        }

        let elapsed: chrono::TimeDelta = Utc::now() - start_time;

        batch_log!(
            info,
            "[BatchServiceImpl::input_batch_by_schedule] {} completed in {}ms",
            index_name,
            elapsed.num_milliseconds()
        );

        Ok(())
    }
}
