//! Monthly spend report delivered via SMTP.
use crate::{batch_log, common::*};

use crate::app_config::AppConfig;

use crate::models::{
    batch_schedule::*, AggResultSet, DocumentWithId, SendEmailAggGroup, SpentDetailIndexing,
};

use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService, indexing_service::IndexingService,
    mysql_service::MysqlService, producer_service::ProducerService,
    public_data_service::PublicDataService, smtp_service::SmtpService,
};

use crate::enums::RangeOperator;

use chrono::Months;

use super::BatchServiceImpl;

/// Escapes reserved HTML characters for safe insertion into HTML.
///
/// Replaces characters that can break markup or introduce unintended HTML
/// with their corresponding entity values before rendering report rows.
///
/// # Arguments
///
/// * `s` - The raw text value to escape
///
/// # Returns
///
/// Returns a new `String` containing the escaped text.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Formats a money amount with comma separators.
///
/// Converts an integer amount into the display format used by the monthly
/// report, such as `1000000` to `1,000,000`.
///
/// # Arguments
///
/// * `amount` - The money amount to format
///
/// # Returns
///
/// Returns the formatted amount as a `String`.
fn format_money(amount: i64) -> String {
    let s = amount.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Formats a UTC date for display in the monthly report.
///
/// Converts a [`DateTime<Utc>`] into the Korean date label used in email rows.
///
/// # Arguments
///
/// * `dt` - The UTC date and time to format
///
/// # Returns
///
/// Returns a date string in `YYYY년 M월 D일` format.
fn format_date(dt: DateTime<Utc>) -> String {
    format!("{}년 {}월 {}일", dt.year(), dt.month(), dt.day())
}

/// Builds HTML table rows for the monthly spend detail list.
///
/// Converts each indexed spend detail document into a table row for the report
/// email. Empty text fields are rendered as `-`, and user-provided text is HTML
/// escaped before insertion.
///
/// # Arguments
///
/// * `source_list` - The spend detail documents returned from Elasticsearch
///
/// # Returns
///
/// Returns the rendered `<tr>` HTML string. If no spend details exist, returns
/// a single empty-state row.
fn build_html_rows(source_list: &[DocumentWithId<SpentDetailIndexing>]) -> String {

    if source_list.is_empty() {
        return "<tr><td colspan=\"5\" style=\"padding:8px 12px;text-align:center;color:#888;\">소비 내역이 없습니다.</td></tr>".to_string();
    }

    let mut html: String = String::new();

    for doc in source_list {
        let item: &SpentDetailIndexing = doc.source();

        let spent_name: String = if item.spent_name().is_empty() {
            "-".to_string()
        } else {
            escape_html(item.spent_name())
        };

        let spent_money: String = format_money(*item.spent_money());
        let spent_at: String = format_date(*item.spent_at());

        let consume_keyword_type: String = if item.consume_keyword_type().is_empty() {
            "-".to_string()
        } else {
            escape_html(item.consume_keyword_type())
        };

        let card_alias: String = if item.card_alias().is_empty() {
            "-".to_string()
        } else {
            escape_html(item.card_alias())
        };

        html.push_str(&format!(
            "<tr>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">{}</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;text-align:right;\">{} 원</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">{}</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">{}</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">{}</td>\
             </tr>",
            spent_name, spent_money, spent_at, consume_keyword_type, card_alias,
        ));
    }

    html
}

/// Renders the monthly report HTML from the configured template.
///
/// Reads the monthly report template path from [`AppConfig`] and replaces date,
/// row, and total placeholders with the calculated report values.
///
/// # Arguments
///
/// * `start_date` - The first date included in the report range
/// * `end_date` - The last date included in the report range
/// * `rows_html` - Pre-rendered HTML table rows for spend details
/// * `total` - Total spend amount for the report range
///
/// # Returns
///
/// Returns the complete HTML email body.
///
/// # Errors
///
/// Returns an error if:
/// - Global application configuration is not initialized
/// - Monthly report template file cannot be read
fn build_monthly_report_html(start_date: DateTime<Utc>, end_date: DateTime<Utc>, rows_html: &str, total: i64) -> anyhow::Result<String> {
    let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
        error!("[report::build_monthly_report_html] Initialisation of `AppConfig` has failed. {:#}", e);
    })?;
    let template_path: &str = app_config.monthly_report_template();

    let template: String = std::fs::read_to_string(template_path)
        .map_err(|e| anyhow!("Failed to read HTML template '{}': {}", template_path, e))?;

    let html = template
        .replace("{{START_YEAR}}", &start_date.year().to_string())
        .replace("{{START_MONTH}}", &start_date.month().to_string())
        .replace("{{START_DAY}}", &start_date.day().to_string())
        .replace("{{END_YEAR}}", &end_date.year().to_string())
        .replace("{{END_MONTH}}", &end_date.month().to_string())
        .replace("{{END_DAY}}", &end_date.day().to_string())
        .replace("{{ROWS}}", rows_html)
        .replace("{{TOTAL}}", &format_money(total));

    Ok(html)
}


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
    /// Calculates the monthly report date range.
    ///
    /// Builds an inclusive range starting at 09:00:00 UTC on the same calendar
    /// day of the previous month and ending at the provided `now` value.
    ///
    /// # Arguments
    ///
    /// * `now` - The current UTC date and time used as the report end date
    ///
    /// # Returns
    ///
    /// Returns `(start_date, end_date)` for the report query range.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The previous-month date cannot be calculated
    /// - The calculated start time cannot be represented
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

    /// Loads report recipient email records grouped by aggregate group sequence.
    ///
    /// Reads send-email aggregate group rows from MySQL in batches, then builds a
    /// map where each `agg_group_seq` points to the email addresses that should
    /// receive that group's report.
    ///
    /// # Arguments
    ///
    /// * `mysql_service` - MySQL service used to fetch recipient aggregate groups
    /// * `batch_size` - Number of recipient rows to fetch per query
    ///
    /// # Returns
    ///
    /// Returns `(agg_group_map, total_processed)`, where `agg_group_map` is keyed
    /// by aggregate group sequence and `total_processed` is the number of rows read.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - MySQL recipient lookup fails for any batch
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

    /// Processes and sends the monthly report for one aggregate group.
    ///
    /// Queries Elasticsearch for the group's spend details, renders the monthly
    /// report HTML, and sends the same report to each email address associated
    /// with the aggregate group.
    ///
    /// # Arguments
    ///
    /// * `agg_group_seq` - Aggregate group sequence used to filter report data
    /// * `email_ids` - Email addresses that should receive the rendered report
    /// * `elastic_service` - Elasticsearch service used to query spend details
    /// * `smtp_service` - SMTP service used to send the report email
    /// * `index_name` - Elasticsearch index or alias to query
    /// * `start_date` - First date included in the report range
    /// * `end_date` - Last date included in the report range
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the report is sent to every recipient.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Elasticsearch report data query fails
    /// - Monthly report HTML template cannot be rendered
    /// - SMTP delivery fails for any recipient
    async fn process_agg_group(
        agg_group_seq: i64,
        email_ids: Vec<String>,
        elastic_service: Arc<E>,
        smtp_service: Arc<S>,
        index_name: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> anyhow::Result<()> {
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
                agg_group_seq,
            )
            .await?;

        let rows_html: String = build_html_rows(cur_agg_infos.source_list());
        let total: i64 = cur_agg_infos.agg_result().round() as i64;

        let html: String = build_monthly_report_html(start_date, end_date, &rows_html, total)?;
        let subject: String = format!(
            "[소비 리포트] {}년 {}월 {}일 ~ {}년 {}월 {}일 소비 내역",
            start_date.year(), start_date.month(), start_date.day(),
            end_date.year(), end_date.month(), end_date.day(),
        );

        for email_id in &email_ids {
            smtp_service
                .send_html_email(email_id, &subject, &html)
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::process_agg_group] Failed to send email to {}: {:#}",
                        email_id, e
                    );
                })?;
        }

        batch_log!(
            info,
            "[BatchServiceImpl::process_agg_group] agg_group_seq={}, email_count={}, sent successfully",
            agg_group_seq,
            email_ids.len()
        );

        Ok(())
    }

    /// Collects the next completed aggregate-group task result.
    ///
    /// Waits for one task in the [`JoinSet`] to finish, logs task errors, and
    /// converts the result into a simple failure flag for the caller.
    ///
    /// # Arguments
    ///
    /// * `join_set` - Running aggregate-group report tasks
    ///
    /// # Returns
    ///
    /// Returns `true` when the completed task failed, otherwise `false`.
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

    /// Processes aggregate-group reports with bounded concurrency.
    ///
    /// Spawns one task per aggregate group and limits the number of in-flight
    /// report sends with `MONTHLY_REPORT_MAX_CONCURRENCY`.
    ///
    /// # Arguments
    ///
    /// * `agg_group_map` - Map of aggregate group sequence to recipient email addresses
    /// * `elastic_service` - Elasticsearch service shared by report tasks
    /// * `smtp_service` - SMTP service shared by report tasks
    /// * `index_name` - Elasticsearch index or alias to query
    /// * `start_date` - First date included in the report range
    /// * `end_date` - Last date included in the report range
    ///
    /// # Returns
    ///
    /// Returns the number of aggregate-group tasks that failed.
    async fn process_agg_groups_concurrently(
        agg_group_map: HashMap<i64, Vec<String>>,
        elastic_service: &Arc<E>,
        smtp_service: &Arc<S>,
        index_name: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> usize {

        const MONTHLY_REPORT_MAX_CONCURRENCY: usize = 10;

        let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let mut failed_count: usize = 0;

        for (agg_group_seq, email_ids) in agg_group_map {
            let elastic_service: Arc<E> = Arc::clone(elastic_service);
            let smtp_service: Arc<S> = Arc::clone(smtp_service);
            let index_name: String = index_name.to_string();

            /*
                - agg_group_seq: Group sequence number.
                - email_ids: Email addresses belonging to the corresponding group.
                In other words, this logic assigns one task per agg_group_seq
                and sends emails to the addresses belonging to each group.
            */
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

    /// Sends monthly spend reports to configured recipients.
    ///
    /// Calculates the monthly report range, loads recipient aggregate groups from
    /// MySQL, queries Elasticsearch for each group's spend details, and sends the
    /// rendered HTML report through SMTP.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - Batch schedule configuration containing batch size and index name
    /// * `elastic_service` - Elasticsearch service used to query spend details
    /// * `mysql_service` - MySQL service used to load report recipients
    /// * `smtp_service` - SMTP service used to send report emails
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after all aggregate groups have been processed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The report date range cannot be calculated
    /// - Recipient aggregate groups cannot be loaded from MySQL
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

        /* Function that executes processes in parallel. */
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
