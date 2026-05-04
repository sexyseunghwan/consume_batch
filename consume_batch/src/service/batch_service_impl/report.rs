//! Monthly spend report delivered via SMTP.
use crate::{batch_log, common::*};

use crate::models::{
    batch_schedule::*, AggResultSet, SendEmailAggGroup, SpentDetailIndexing
};

use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService, indexing_service::IndexingService,
    mysql_service::MysqlService, producer_service::ProducerService,
    public_data_service::PublicDataService, smtp_service::SmtpService, 
};

use crate::enums::{RangeOperator};

use chrono::Months;

use super::BatchServiceImpl;

const MONTHLY_REPORT_MAX_CONCURRENCY: usize = 10;


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
    fn find_monthly_report_range(now: DateTime<Utc>) -> anyhow::Result<(DateTime<Utc>, DateTime<Utc>)> {
        let start_date_naive: chrono::NaiveDateTime = now
            .date_naive()
            .checked_sub_months(Months::new(1))
            .and_then(|date| date.and_hms_opt(9, 0, 0))
            .ok_or_else(|| {
                anyhow!(
                    "[BatchServiceImpl::send_monthly_spent_report] Failed to calculate one month ago 09:00:00 from {}",
                    now
                )
            })?;

        let start_date: DateTime<Utc> =
            DateTime::<Utc>::from_naive_utc_and_offset(start_date_naive, Utc);

        Ok((start_date, now))
    }

    async fn find_send_email_agg_group_map(
        mysql_service: &Arc<M>,
        batch_size: u64,
    ) -> anyhow::Result<(HashMap<i64, Vec<String>>, u64)> {
        let mut agg_group_map: HashMap<i64, Vec<String>> = HashMap::new();
        let mut offset: u64 = 0;
        let mut total_processed: u64 = 0;

        loop {
            let send_infos: Vec<SendEmailAggGroup> = mysql_service
                .find_send_email_agg_group(offset, batch_size)
                .await
                .inspect_err(|e| {
                    error!(
                        "[reports::send_monthly_spent_report] Failed to fetch send email agg group: {:#}",
                        e
                    );
                })?;

            if send_infos.is_empty() {
                break;
            }

            total_processed += send_infos.len() as u64;

            for send_info in send_infos {
                let agg_group_seq: i64 = *send_info.agg_group_seq();
                let email_id: String = send_info.email_id().clone();

                agg_group_map
                    .entry(agg_group_seq)
                    .or_default()
                    .push(email_id);
            }

            offset += batch_size;
        }

        Ok((agg_group_map, total_processed))
    }

    async fn process_agg_group(
        agg_group_seq: i64,
        email_ids: Vec<String>,
        elastic_service: Arc<E>,
        _smtp_service: Arc<S>,
        index_name: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // TODO: agg_group_seq 기준 실제 리포트 조회/가공/전송 로직은 여기서 구현한다.
        //let _ = (elastic_service, smtp_service);
        let cur_agg_infos: AggResultSet<SpentDetailIndexing> = elastic_service
            .find_info_filter_groupseq_orderby_aggs_range(
                index_name, 
                "spent_at", 
                start_date, 
                end_date, 
                RangeOperator::GreaterThanOrEqual, 
                RangeOperator::LessThanOrEqual, 
                "spent_at", 
                true, 
                "spent_money", 
                agg_group_seq
            )
            .await?;
        
        
        //println!("{:?}", cur_agg_infos);
        //println!("test");

        batch_log!(
            info,
            "[BatchServiceImpl::process_agg_group] agg_group_seq={}, email_count={}",
            agg_group_seq,
            email_ids.len()
        );

        Ok(())
    }

    async fn collect_finished_agg_group_task(
        join_set: &mut JoinSet<anyhow::Result<()>>,
    ) -> bool {
        match join_set.join_next().await {
            Some(Ok(Ok(()))) => false,
            Some(Ok(Err(e))) => {
                error!(
                    "[BatchServiceImpl::send_monthly_spent_report] process_agg_group failed: {:#}",
                    e
                );
                // 실패 즉시 전체 프로세스를 중단하려면 호출부에서 Err를 반환하도록 바꾼다.
                true
            }
            Some(Err(e)) => {
                error!(
                    "[BatchServiceImpl::send_monthly_spent_report] process_agg_group task join failed: {:#}",
                    e
                );
                // task join 실패도 즉시 중단하려면 호출부에서 Err를 반환하도록 바꾼다.
                true
            }
            None => false,
        }
    }

    async fn process_agg_groups_concurrently(
        agg_group_map: HashMap<i64, Vec<String>>,
        elastic_service: &Arc<E>,
        smtp_service: &Arc<S>,
        index_name: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> usize {
        let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let mut failed_count: usize = 0;

        for (agg_group_seq, email_ids) in agg_group_map {
            let elastic_service: Arc<E> = Arc::clone(elastic_service);
            let smtp_service: Arc<S> = Arc::clone(smtp_service);
            let index_name: String = index_name.to_string();

            join_set.spawn(async move {
                Self::process_agg_group(
                    agg_group_seq,
                    email_ids,
                    elastic_service,
                    smtp_service,
                    &index_name,
                    start_date,
                    end_date,
                )
                .await
            });

            if join_set.len() >= MONTHLY_REPORT_MAX_CONCURRENCY
                && Self::collect_finished_agg_group_task(&mut join_set).await
            {
                failed_count += 1;
            }
        }

        while !join_set.is_empty() {
            if Self::collect_finished_agg_group_task(&mut join_set).await {
                failed_count += 1;
            }
        }

        failed_count
    }

    /// Fetches the previous month's spend summary and emails each user their report.
    ///
    /// Queries `SPENT_DETAIL_INDEXING` for the month prior to today, groups results
    /// by user, and sends one HTML email per user via the SMTP service.
    ///
    /// # Arguments
    ///
    /// * `mysql_service` - MySQL service for querying monthly spend summaries
    /// * `smtp_service` - SMTP service for sending the report emails
    ///
    /// # Errors
    ///
    /// Returns an error if the MySQL query or SMTP delivery fails.
    pub(super) async fn send_monthly_spent_report(
        schedule_item: &BatchScheduleItem,
        elastic_service: &Arc<E>,
        mysql_service: &Arc<M>,
        smtp_service: &Arc<S>,
    ) -> anyhow::Result<()> {
        let now: DateTime<Utc> = Utc::now();
        let (start_date, end_date) = Self::find_monthly_report_range(now)?;

        batch_log!(
            info,
            "[BatchServiceImpl::send_monthly_spent_report] Generating report from {} to {}",
            start_date,
            end_date
        );

        let batch_size: u64 = *schedule_item.batch_size() as u64;   
        let index_name: String = format!("read_{}", schedule_item.index_name());
        let (agg_group_map, total_processed) =
            Self::find_send_email_agg_group_map(mysql_service, batch_size).await?;

        batch_log!(
            info,
            "[BatchServiceImpl::send_monthly_spent_report] Loaded {} send-email agg group rows, active agg groups={}",
            total_processed,
            agg_group_map.len()
        );


        // 여기서 이제 elasticsearch에서 데이터를 가져와준다.
        let failed_count: usize = Self::process_agg_groups_concurrently(
            agg_group_map,
            elastic_service,
            smtp_service,
            &index_name,
            start_date,
            end_date,
        )
        .await;

        if failed_count > 0 {
            warn!(
                "[BatchServiceImpl::send_monthly_spent_report] Completed with {} failed agg group task(s)",
                failed_count
            );
        }

        Ok(())
    }
}
