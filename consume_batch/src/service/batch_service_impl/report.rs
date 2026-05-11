//! Monthly spend report delivered via SMTP.
use crate::{batch_log, common::*};

use crate::app_config::AppConfig;

use crate::dtos::{GroupSeqAggsRangeQuery, ReportDateRange};

use crate::models::{
    AggResultSet, DocumentWithId, SendEmailAggGroup, SpentDetailIndexing, SpentResultByType,
    batch_schedule::*,
};

use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService,
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

/// Builds HTML table rows for spend totals grouped by category.
///
/// Converts calculated category summary values into table rows for the report
/// email. Empty category names are rendered as `-`, and category names are HTML
/// escaped before insertion.
///
/// # Arguments
///
/// * `detail_by_type` - Category-level spend summaries to render
///
/// # Returns
///
/// Returns the rendered `<tr>` HTML string. If no category summaries exist,
/// returns a single empty-state row.
fn build_category_rows(detail_by_type: &[SpentResultByType]) -> String {
    if detail_by_type.is_empty() {
        return "<tr><td colspan=\"3\" style=\"padding:8px 12px;text-align:center;color:#888;\">카테고리 데이터가 없습니다.</td></tr>".to_string();
    }

    let mut html: String = String::new();
    for item in detail_by_type {
        let spent_type: String = if item.spent_type().is_empty() {
            "-".to_string()
        } else {
            escape_html(item.spent_type())
        };
        let spent_cost: String = format_money(*item.spent_cost());
        let spent_per: String = format!("{:.1}%", item.spent_per());

        html.push_str(&format!(
            "<tr>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">{}</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;text-align:right;\">{} 원</td>\
               <td style=\"padding:8px 12px;border-bottom:1px solid #eee;text-align:right;\">{}</td>\
             </tr>",
            spent_type, spent_cost, spent_per,
        ));
    }
    html
}

/// Builds an HTML table comparing current and previous period spend totals.
///
/// Renders a single-row summary table with three columns — current total,
/// previous total, and the change expressed as a percentage and absolute
/// amount.  The change cell is colour-coded: red when spending increased,
/// green when it decreased, and grey when it is unchanged or there is no
/// previous data.
///
/// # Arguments
///
/// * `cur_total` - Total spend amount for the current report period
/// * `prev_total` - Total spend amount for the comparison period; pass `0`
///   when no previous data is available
///
/// # Returns
///
/// Returns a complete `<table>` HTML string ready for template injection.
fn build_period_summary_html(cur_total: i64, prev_total: i64) -> String {
    let cur_str: String = format!("{} 원", format_money(cur_total));
    let prev_str: String = if prev_total == 0 {
        "-".to_string()
    } else {
        format!("{} 원", format_money(prev_total))
    };

    let (color, change_str) = if prev_total == 0 {
        ("#888888", "-".to_string())
    } else {
        let diff: i64 = cur_total - prev_total;
        let diff_pct: f64 = ((diff as f64 / prev_total as f64) * 1000.0).round() / 10.0;
        if diff > 0 {
            (
                "#e53935",
                format!("▲ +{:.1}% (+{} 원)", diff_pct, format_money(diff)),
            )
        } else if diff < 0 {
            (
                "#43a047",
                format!("▼ {:.1}% (-{} 원)", diff_pct, format_money(-diff)),
            )
        } else {
            ("#888888", "━ 0.0% (0 원)".to_string())
        }
    };

    format!(
        "<table style=\"width:100%;border-collapse:collapse;margin-top:16px;\">\
           <thead>\
             <tr style=\"background:#f0f4ff;\">\
               <th style=\"padding:8px 12px;text-align:right;color:#555;font-weight:600;\">이번 기간 총소비</th>\
               <th style=\"padding:8px 12px;text-align:right;color:#555;font-weight:600;\">지난 기간 총소비</th>\
               <th style=\"padding:8px 12px;text-align:right;color:#555;font-weight:600;\">등락</th>\
             </tr>\
           </thead>\
           <tbody>\
             <tr>\
               <td style=\"padding:8px 12px;text-align:right;\">{cur_str}</td>\
               <td style=\"padding:8px 12px;text-align:right;\">{prev_str}</td>\
               <td style=\"padding:8px 12px;text-align:right;color:{color};font-weight:bold;\">{change_str}</td>\
             </tr>\
           </tbody>\
         </table>"
    )
}

/// Renders a spend report HTML body from the configured template.
///
/// Reads the report template path from [`AppConfig`] and substitutes all
/// placeholders with the supplied values.  Placeholder mapping:
///
/// | Placeholder         | Value                                     |
/// |---------------------|-------------------------------------------|
/// | `{{REPORT_TITLE}}`  | `report_title`                            |
/// | `{{START_*}}`       | Year / month / day of `start_date`        |
/// | `{{END_*}}`         | Year / month / day of `end_date`          |
/// | `{{COMPARISON}}`    | Inline comparison text (`comparison_html`) |
/// | `{{PERIOD_SUMMARY}}`| Period-vs-period summary table            |
/// | `{{ROWS}}`          | Spend detail table rows                   |
/// | `{{CATEGORY_ROWS}}` | Category summary table rows               |
/// | `{{TOTAL}}`         | Grand total formatted with commas         |
///
/// # Arguments
///
/// * `report_title` - Label injected into `{{REPORT_TITLE}}` (e.g. "월간 소비 리포트")
/// * `start_date` - First date included in the report range
/// * `end_date` - Last date included in the report range
/// * `rows_html` - Pre-rendered `<tr>` rows for the spend detail table
/// * `total` - Grand total spend amount for the current period
/// * `category_rows_html` - Pre-rendered `<tr>` rows for the category summary table
/// * `comparison_html` - Inline HTML fragment showing the period-over-period change
/// * `period_summary_html` - HTML table comparing current and previous period totals
///
/// # Errors
///
/// Returns an error if the global config is not initialized or the template
/// file cannot be read.
fn build_report_html(
    report_title: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    rows_html: &str,
    total: i64,
    category_rows_html: &str,
    period_summary_html: &str,
) -> anyhow::Result<String> {
    let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
        error!(
            "[BatchServiceImpl::build_report_html] AppConfig not initialized: {:#}",
            e
        );
    })?;
    let template_path: &str = app_config.monthly_report_template();

    let template: String = std::fs::read_to_string(template_path).map_err(|e| {
        anyhow!(
            "[BatchServiceImpl::build_report_html] Failed to read HTML template '{}': {}",
            template_path,
            e
        )
    })?;

    let html: String = template
        .replace("{{REPORT_TITLE}}", report_title)
        .replace("{{START_YEAR}}", &start_date.year().to_string())
        .replace("{{START_MONTH}}", &start_date.month().to_string())
        .replace("{{START_DAY}}", &start_date.day().to_string())
        .replace("{{END_YEAR}}", &end_date.year().to_string())
        .replace("{{END_MONTH}}", &end_date.month().to_string())
        .replace("{{END_DAY}}", &end_date.day().to_string())
        .replace("{{PERIOD_SUMMARY}}", period_summary_html)
        .replace("{{ROWS}}", rows_html)
        .replace("{{CATEGORY_ROWS}}", category_rows_html)
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
    fn find_monthly_report_range(
        now: DateTime<Utc>,
    ) -> anyhow::Result<(DateTime<Utc>, DateTime<Utc>)> {
        let start_date_naive: chrono::NaiveDateTime = now
            .date_naive()
            .checked_sub_months(Months::new(1))
            .and_then(|date| date.and_hms_opt(9, 0, 0))
            .ok_or_else(|| {
                anyhow!(
                    "[BatchServiceImpl::find_monthly_report_range] Failed to calculate one month ago 09:00:00 from {}",
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
                        "[BatchServiceImpl::find_send_email_agg_group_map] Failed to fetch send email agg group: {:#}",
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

    /// Calculates report pie-chart/category summary values from category totals.
    ///
    /// Converts a map of category name to total spend amount into
    /// `SpentResultByType` values, calculating each category's percentage of the
    /// total report cost and rounding it to one decimal place.
    ///
    /// # Arguments
    ///
    /// * `total_cost` - Total spend amount across all categories
    /// * `type_map` - Map of category name to category spend amount
    ///
    /// # Returns
    ///
    /// Returns category spend summary values used by the report template.
    ///
    /// # Errors
    ///
    /// This function currently does not create errors directly, but returns
    /// `anyhow::Result` to match the surrounding report helper pipeline.
    fn find_calculate_pie_infos_from_category(
        total_cost: f64,
        type_map: &HashMap<String, i64>,
    ) -> anyhow::Result<Vec<SpentResultByType>> {
        let spent_result_by_types: Vec<SpentResultByType> = type_map
            .iter()
            .filter(|(_, value)| **value > 0)
            .map(|(key, value)| {
                let spent_type: String = key.to_string();
                let spent_cost: i64 = *value;
                
                let spent_per: f64 = (spent_cost as f64 / total_cost) * 100.0;
                let spent_per_rounded: f64 = (spent_per * 10.0).round() / 10.0; /* Round to the second decimal place */

                SpentResultByType::new(spent_type, spent_cost, spent_per_rounded)
            })
            .collect();

        Ok(spent_result_by_types)
    }

    /// Builds sorted category summaries from spend detail search results.
    ///
    /// Groups Elasticsearch spend detail results by `consume_keyword_type`, sums
    /// spend amounts for each category, calculates category percentages, and
    /// sorts the result by spend amount in descending order.
    ///
    /// # Arguments
    ///
    /// * `spent_details` - Aggregated spend details returned from Elasticsearch
    ///
    /// # Returns
    ///
    /// Returns category spend summaries sorted from highest spend to lowest.
    ///
    /// # Errors
    ///
    /// Returns an error if category percentage calculation fails.
    fn find_spent_result_by_category(
        spent_details: &AggResultSet<SpentDetailIndexing>,
    ) -> anyhow::Result<Vec<SpentResultByType>> {
        let spent_inner_details: &Vec<DocumentWithId<SpentDetailIndexing>> =
            spent_details.source_list();
        let total_cost: f64 = *spent_details.agg_result();

        let mut cost_map: HashMap<String, i64> =
            spent_inner_details
                .iter()
                .fold(HashMap::new(), |mut acc, spent_detail| {
                    let spent_detail: &SpentDetailIndexing = spent_detail.source();
                    let spent_type: String = spent_detail.consume_keyword_type().to_string();
                    let spent_money: i64 = spent_detail.spent_money;

                    acc.entry(spent_type)
                        .and_modify(|value| *value += spent_money)
                        .or_insert(spent_money);

                    acc
                });

        cost_map.retain(|_, v| *v > 0);

        let mut spent_result_by_types: Vec<SpentResultByType> =
            Self::find_calculate_pie_infos_from_category(total_cost, &cost_map)?;

        spent_result_by_types.sort_by(|a, b| {
            b.spent_cost
                .partial_cmp(&a.spent_cost)
                .unwrap_or(Ordering::Equal)
        });

        Ok(spent_result_by_types)
    }

    /// Processes and sends a spend report for one aggregate group.
    ///
    /// Queries Elasticsearch twice — once for the current period and once for
    /// the comparison period — then builds the full HTML report (spend detail
    /// rows, category summary, period-vs-period comparison) and delivers it to
    /// every email address associated with the aggregate group.
    ///
    /// # Arguments
    ///
    /// * `agg_group_seq` - Aggregate group sequence used to filter Elasticsearch data
    /// * `email_ids` - Email addresses that should receive the rendered report
    /// * `elastic_service` - Elasticsearch service used to query spend details
    /// * `smtp_service` - SMTP service used to deliver the report email
    /// * `report_title` - Report label shown in the email subject and HTML header
    ///   (e.g. "월간 소비 리포트" or "주간 소비 리포트")
    /// * `index_name` - Elasticsearch index or alias to query
    /// * `date_range` - Date range for the current report period
    /// * `prev_date_range` - Date range for the comparison (previous) period;
    ///   used to calculate the period-over-period change shown in the report
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the report is delivered to every recipient.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either Elasticsearch query fails
    /// - The HTML report cannot be rendered (config not initialized or template unreadable)
    /// - SMTP delivery fails for any recipient
    async fn process_agg_group(
        agg_group_seq: i64,
        email_ids: Vec<String>,
        elastic_service: Arc<E>,
        smtp_service: Arc<S>,
        report_title: String,
        index_name: &str,
        date_range: ReportDateRange,
        prev_date_range: ReportDateRange
    ) -> anyhow::Result<()> {
        // 소비 정보 디테일 + 집계
        let cur_agg_infos: AggResultSet<SpentDetailIndexing> = elastic_service
            .find_info_filter_groupseq_orderby_aggs_range(GroupSeqAggsRangeQuery {
                index_name,
                range_field: "spent_at",
                start_date: date_range.start_date,
                end_date: date_range.end_date,
                start_op: RangeOperator::GreaterThanOrEqual,
                end_op: RangeOperator::LessThanOrEqual,
                order_by_field: "spent_at",
                asc_yn: true,
                aggs_field: "spent_money",
                group_seq: agg_group_seq,
                query_size: 10000
            })
            .await?;
        

        // 비교 기간 집계
        let versus_agg_infos: AggResultSet<SpentDetailIndexing> = elastic_service
            .find_info_filter_groupseq_orderby_aggs_range(GroupSeqAggsRangeQuery {
                index_name,
                range_field: "spent_at",
                start_date: prev_date_range.start_date,
                end_date: prev_date_range.end_date,
                start_op: RangeOperator::GreaterThanOrEqual,
                end_op: RangeOperator::LessThanOrEqual,
                order_by_field: "spent_at",
                asc_yn: true,
                aggs_field: "spent_money",
                group_seq: agg_group_seq,
                query_size: 0
            })
            .await?;
        
        // 소비 정보 디테일 - 카테고리 별
        let detail_by_type: Vec<SpentResultByType> = Self::find_spent_result_by_category(&cur_agg_infos)
            .inspect_err(|e| {
                error!("[BatchServiceImpl::process_agg_group] Initialisation of the `detail_by_type` data has failed. {:#}", e);
            })?;

        let rows_html: String = build_html_rows(cur_agg_infos.source_list());
        let category_rows_html: String = build_category_rows(&detail_by_type);
        let total: i64 = cur_agg_infos.agg_result().round() as i64;
        let prev_total: i64 = versus_agg_infos.agg_result().round() as i64;
        let period_summary_html: String = build_period_summary_html(total, prev_total);

        let html: String =
            build_report_html(
                &report_title,
                date_range.start_date,
                date_range.end_date,
                &rows_html,
                total,
                &category_rows_html,
                &period_summary_html,
            )?;
        let subject: String = format!(
            "[{}] {}년 {}월 {}일 ~ {}년 {}월 {}일 소비 내역",
            report_title,
            date_range.start_date.year(),
            date_range.start_date.month(),
            date_range.start_date.day(),
            date_range.end_date.year(),
            date_range.end_date.month(),
            date_range.end_date.day(),
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
    async fn collect_finished_agg_group_task(join_set: &mut JoinSet<anyhow::Result<()>>) -> bool {
        match join_set.join_next().await {
            Some(Ok(Ok(()))) => false,
            Some(Ok(Err(e))) => {
                error!(
                    "[BatchServiceImpl::collect_finished_agg_group_task] process_agg_group failed: {:#}",
                    e
                );
                // 실패 즉시 전체 프로세스를 중단하려면 호출부에서 Err를 반환하도록 바꾼다.
                true
            }
            Some(Err(e)) => {
                error!(
                    "[BatchServiceImpl::collect_finished_agg_group_task] process_agg_group task join failed: {:#}",
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
    /// Spawns one task per aggregate group and caps simultaneous in-flight
    /// sends at `REPORT_MAX_CONCURRENCY`.  Each task calls
    /// [`Self::process_agg_group`], which queries both the current and
    /// comparison periods, renders the full HTML report, and delivers it via
    /// SMTP.
    ///
    /// # Arguments
    ///
    /// * `agg_group_map` - Map of aggregate group sequence to recipient email addresses
    /// * `elastic_service` - Elasticsearch service shared across all report tasks
    /// * `smtp_service` - SMTP service shared across all report tasks
    /// * `report_title` - Label forwarded to each task (e.g. "월간 소비 리포트")
    /// * `index_name` - Elasticsearch index or alias to query
    /// * `date_range` - Date range for the current report period
    /// * `prev_date_range` - Date range for the comparison period, forwarded to
    ///   each task for period-over-period calculations
    ///
    /// # Returns
    ///
    /// Returns the number of aggregate-group tasks that failed.
    async fn process_agg_groups_concurrently(
        agg_group_map: HashMap<i64, Vec<String>>,
        elastic_service: &Arc<E>,
        smtp_service: &Arc<S>,
        report_title: String,
        index_name: &str,
        date_range: ReportDateRange,
        prev_date_range: ReportDateRange
    ) -> usize {
        const REPORT_MAX_CONCURRENCY: usize = 10;

        let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();
        let mut failed_count: usize = 0;

        for (agg_group_seq, email_ids) in agg_group_map {
            let elastic_service: Arc<E> = Arc::clone(elastic_service);
            let smtp_service: Arc<S> = Arc::clone(smtp_service);
            let report_title: String = report_title.clone();
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
                    report_title,
                    &index_name,
                    date_range,
                    prev_date_range
                )
                .await
            });

            if join_set.len() >= REPORT_MAX_CONCURRENCY
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
        let date_range: ReportDateRange = ReportDateRange { start_date, end_date };

        let (prev_start_date, prev_end_date) = Self::find_monthly_report_range(start_date)?;
        let prev_date_range: ReportDateRange = ReportDateRange {
            start_date: prev_start_date,
            end_date: prev_end_date,
        };

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
            "월간 소비 리포트".to_string(),
            &index_name,
            date_range,
            prev_date_range
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

    /// Calculates the weekly report date range.
    ///
    /// Builds an inclusive range starting at 09:00:00 UTC seven days before
    /// `now` and ending at the provided `now` value.
    ///
    /// # Arguments
    ///
    /// * `now` - The current UTC date and time used as the report end date
    ///
    /// # Returns
    ///
    /// Returns `(start_date, end_date)` for the weekly report query range.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Seven days before `now` cannot be calculated
    /// - The calculated start time cannot be represented
    fn find_weekly_report_range(
        now: DateTime<Utc>,
    ) -> anyhow::Result<(DateTime<Utc>, DateTime<Utc>)> {
        let seven_days_ago: DateTime<Utc> = now
            .checked_sub_signed(chrono::Duration::days(7))
            .ok_or_else(|| anyhow!("[BatchServiceImpl::find_weekly_report_range] Failed to calculate 7 days ago from {}", now))?;

        let start_date_naive = seven_days_ago
            .date_naive()
            .and_hms_opt(9, 0, 0)
            .ok_or_else(|| anyhow!("[BatchServiceImpl::find_weekly_report_range] Failed to create 09:00:00 for date {:?}", seven_days_ago.date_naive()))?;

        let start_date = DateTime::<Utc>::from_naive_utc_and_offset(start_date_naive, Utc);

        Ok((start_date, now))
    }

    /// Sends weekly spend reports to configured recipients.
    ///
    /// Calculates two consecutive weekly date ranges — the current week
    /// (`date_range`) and the preceding week (`prev_date_range`) — then loads
    /// recipient aggregate groups from MySQL and dispatches one HTML report per
    /// group via SMTP.  Each report includes spend details, a category summary,
    /// and a period-over-period comparison between the two weeks.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - Batch schedule configuration (batch size, index name)
    /// * `elastic_service` - Elasticsearch service used to query spend details
    /// * `mysql_service` - MySQL service used to load report recipients
    /// * `smtp_service` - SMTP service used to deliver report emails
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after all aggregate groups have been processed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either weekly date range cannot be calculated
    /// - Recipient aggregate groups cannot be loaded from MySQL
    pub(super) async fn send_weekly_spent_report(
        schedule_item: &BatchScheduleItem,
        elastic_service: &Arc<E>,
        mysql_service: &Arc<M>,
        smtp_service: &Arc<S>,
    ) -> anyhow::Result<()> {
        let now: DateTime<Utc> = Utc::now();
        let (start_date, end_date) = Self::find_weekly_report_range(now)?;
        let date_range: ReportDateRange = ReportDateRange { start_date, end_date };

        let (prev_start_date, prev_end_date) = Self::find_weekly_report_range(start_date)?;
        let prev_date_range: ReportDateRange = ReportDateRange {
            start_date: prev_start_date,
            end_date: prev_end_date,
        };

        batch_log!(
            info,
            "[BatchServiceImpl::send_weekly_spent_report] Generating report from {} to {}",
            start_date,
            end_date
        );

        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let index_name: String = format!("read_{}", schedule_item.index_name());
        let (agg_group_map, total_processed) =
            Self::find_send_email_agg_group_map(mysql_service, batch_size).await?;

        batch_log!(
            info,
            "[BatchServiceImpl::send_weekly_spent_report] Loaded {} send-email agg group rows, active agg groups={}",
            total_processed,
            agg_group_map.len()
        );

        let failed_count: usize = Self::process_agg_groups_concurrently(
            agg_group_map,
            elastic_service,
            smtp_service,
            "주간 소비 리포트".to_string(),
            &index_name,
            date_range,
            prev_date_range
        )
        .await;

        if failed_count > 0 {
            warn!(
                "[BatchServiceImpl::send_weekly_spent_report] Completed with {} failed agg group task(s)",
                failed_count
            );
        }

        Ok(())
    }
}
