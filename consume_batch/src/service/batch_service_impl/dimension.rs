//! Populate the `DIM_CALENDAR` date dimension table.

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
    pub(super) async fn input_date_dimension_data(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        public_data_service: &Arc<D>,
    ) -> anyhow::Result<()> {
        let start_year: i32 = *schedule_item.start_year();
        let end_year: i32 = *schedule_item.end_year();
        let batch_size: usize = *schedule_item.batch_size();

        batch_log!(
            info,
            "[BatchServiceImpl::input_date_dimension_data] Starting. range={}-{}, batch_size={}",
            start_year,
            end_year,
            batch_size
        );

        let start_date: NaiveDate = NaiveDate::from_ymd_opt(start_year, 1, 1)
            .ok_or_else(|| anyhow!("Invalid start_year: {}", start_year))?;
        let end_date: NaiveDate = NaiveDate::from_ymd_opt(end_year, 12, 31)
            .ok_or_else(|| anyhow!("Invalid end_year: {}", end_year))?;

        fn days_in_month(year: i32, month: u32) -> anyhow::Result<u32> {
            let first_of_next: NaiveDate = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1)
            }
            .ok_or_else(|| anyhow!("Invalid date: year={} month={}", year, month))?;

            let last: NaiveDate = first_of_next
                .pred_opt()
                .ok_or_else(|| anyhow!("Date underflow at {:?}", first_of_next))?;

            Ok(last.day())
        }

        batch_log!(
            info,
            "[BatchServiceImpl::input_date_dimension_data] Fetching Korean holidays from public data API"
        );

        let holiday_set: HashSet<NaiveDate> = match public_data_service
            .find_korea_holiday_set(start_year, end_year)
            .await
        {
            Ok(set) => {
                batch_log!(
                    info,
                    "[BatchServiceImpl::input_date_dimension_data] Fetched {} holiday dates",
                    set.len()
                );
                set
            }
            Err(e) => {
                error!(
                    "[BatchServiceImpl::input_date_dimension_data] Holiday API failed, proceeding without holiday data: {:#}",
                    e
                );
                HashSet::new()
            }
        };

        let mut current: NaiveDate = start_date;
        let mut batch: Vec<crate::entity::dim_calendar::ActiveModel> =
            Vec::with_capacity(batch_size);
        let mut total_inserted: u64 = 0;

        while current <= end_date {
            let yyyy: i16 = current.year() as i16;
            let mm: i8 = current.month() as i8;
            let dd: i8 = current.day() as i8;
            let last_day: u32 = days_in_month(current.year(), current.month())?;

            let weekday_no: i8 = current.weekday().num_days_from_monday() as i8;
            let is_weekend: i8 = if weekday_no >= 5 { 1 } else { 0 };
            let now: chrono::NaiveDateTime = Utc::now().naive_utc();

            let is_holiday: i8 = if holiday_set.contains(&current) { 1 } else { 0 };
            let is_before_holiday: i8 = current
                .succ_opt()
                .map(|next| if holiday_set.contains(&next) { 1 } else { 0 })
                .unwrap_or(0);
            let is_after_holiday: i8 = current
                .pred_opt()
                .map(|prev| if holiday_set.contains(&prev) { 1 } else { 0 })
                .unwrap_or(0);

            batch.push(crate::entity::dim_calendar::ActiveModel {
                dt: Set(current),
                yyyy: Set(yyyy),
                mm: Set(mm),
                dd: Set(dd),
                yyyymm: Set(format!("{:04}{:02}", yyyy, mm)),
                yyyymmdd: Set(format!("{:04}{:02}{:02}", yyyy, mm, dd)),
                day_of_month: Set(dd),
                quarter_no: Set((mm - 1) / 3 + 1),
                half_no: Set(if mm <= 6 { 1 } else { 2 }),
                weekday_no: Set(weekday_no),
                is_weekend: Set(is_weekend),
                is_weekday: Set(1 - is_weekend),
                is_month_start: Set(if dd <= 4 { 1 } else { 0 }),
                is_month_end: Set(if dd as u32 >= last_day - 3 { 1 } else { 0 }),
                remaining_days_in_month: Set((last_day - dd as u32) as i8),
                is_holiday: Set(is_holiday),
                is_before_holiday: Set(is_before_holiday),
                is_after_holiday: Set(is_after_holiday),
                created_at: Set(now),
                updated_at: Set(None),
                created_by: Set("batch".to_string()),
                updated_by: Set(None),
            });

            if batch.len() >= batch_size {
                let count: usize = batch.len();
                mysql_service
                    .input_dim_calendar_bulk(std::mem::take(&mut batch))
                    .await
                    .inspect_err(|e| {
                        error!("[BatchServiceImpl::input_date_dimension_data] Bulk insert failed: {:#}", e);
                    })?;
                total_inserted += count as u64;
                batch_log!(
                    info,
                    "[BatchServiceImpl::input_date_dimension_data] Inserted {} rows (total so far: {})",
                    count,
                    total_inserted
                );
            }

            current = current
                .succ_opt()
                .ok_or_else(|| anyhow!("Date overflow at {:?}", current))?;
        }

        // Flush remaining rows
        if !batch.is_empty() {
            let count: usize = batch.len();
            mysql_service
                .input_dim_calendar_bulk(batch)
                .await
                .inspect_err(|e| {
                    error!("[BatchServiceImpl::input_date_dimension_data] Final bulk insert failed: {:#}", e);
                })?;
            total_inserted += count as u64;
        }

        batch_log!(
            info,
            "[BatchServiceImpl::input_date_dimension_data] Completed. total_inserted={}",
            total_inserted
        );

        Ok(())
    }
}
