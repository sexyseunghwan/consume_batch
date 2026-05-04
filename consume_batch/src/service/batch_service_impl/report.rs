//! Monthly spend report delivered via SMTP.

use crate::{batch_log, common::*};

use crate::models::{
    UserMonthlySpentSummary, batch_schedule::*, SendEmailAggGroup
};

use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService, indexing_service::IndexingService,
    mysql_service::MysqlService, producer_service::ProducerService,
    public_data_service::PublicDataService, smtp_service::SmtpService, 
};

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

        // Report covers the previous calendar month.
        let (year, month) = if now.month() == 1 {
            (now.year() - 1, 12u32)
        } else {
            (now.year(), now.month() - 1)
        };

        batch_log!(
            info,
            "[BatchServiceImpl::send_monthly_spent_report] Generating report for {}-{:02}",
            year, month
        );

        let batch_size: u64 = *schedule_item.batch_size() as u64;   
        let index_name: String = format!("read_{}", schedule_item.index_name());

        let mut agg_group_map: HashMap<i64, String> = HashMap::new();    
        let mut offset: u64 = 0;
        let mut total_processed: u64 = 0;
        

        // SEND_EMAIL_AGG_GROUP 데이터를 가져와준다.
        loop {

            let send_infos: Vec<SendEmailAggGroup> = match mysql_service
                .find_send_email_agg_group(offset, batch_size).await {
                    Ok(send_infos) => send_infos,
                    Err(e) => {
                        error!("[reports::send_monthly_spent_report] Failed to fetch send email agg group: {:#}", e);
                        return Err(e);
                    }
                };

            if send_infos.is_empty() {
                break;
            }
            
            total_processed += send_infos.len() as u64;

            for send_info in send_infos {
                agg_group_map.insert(*send_info.agg_group_seq(), send_info.email_id().clone());
            }
            
            offset += batch_size;
        }

        batch_log!(
            info,
            "[BatchServiceImpl::send_monthly_spent_report] Loaded {} send-email agg group rows, active agg groups={}",
            total_processed,
            agg_group_map.len()
        );


        // 여기서 이제 elasticsearch에서 데이터를 가져와준다.
        

        // let summaries: Vec<UserMonthlySpentSummary> = mysql_service
        //     .find_users_monthly_spent_summary(year, month)
        //     .await
        //     .inspect_err(|e| {
        //         error!("[BatchServiceImpl::send_monthly_spent_report] Failed to fetch summaries: {:#}", e);
        //     })?;

        // if summaries.is_empty() {
        //     batch_log!(
        //         info,
        //         "[BatchServiceImpl::send_monthly_spent_report] No data found for {}-{:02}, skipping",
        //         year, month
        //     );
        //     return Ok(());
        // }

        // smtp_service
        //     .send_monthly_spent_report(year, month, &summaries)
        //     .await
        //     .inspect_err(|e| {
        //         error!("[BatchServiceImpl::send_monthly_spent_report] Failed to send emails: {:#}", e);
        //     })?;

        // batch_log!(
        //     info,
        //     "[BatchServiceImpl::send_monthly_spent_report] Report sent to {} user(s) for {}-{:02}",
        //     summaries.iter().map(|s| s.user_id()).collect::<std::collections::HashSet<_>>().len(),
        //     year, month
        // );

        Ok(())
    }
}
