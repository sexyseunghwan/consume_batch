//! Batch service implementation.
//!
//! This module provides the concrete implementation of [`BatchService`] trait,
//! coordinating MySQL data retrieval, Elasticsearch indexing, and message consumption
//! based on configured schedules.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         BatchServiceImpl                                │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                          JobScheduler                                   │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                      │
//! │  │   Job #1    │  │   Job #2    │  │   Job #3    │  ...                 │
//! │  │ index_dev1  │  │ index_dev2  │  │ index_dev3  │                      │
//! │  │  (cron)     │  │  (cron)     │  │             │                      │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                      │
//! │         │                │                │                             │
//! │         └────────────────┼────────────────┘                             │
//! │                          ▼                                              │
//! │              ┌───────────────────────┐                                  │
//! │              │  process_index_batch  │  (parallel execution)            │
//! │              └───────────────────────┘                                  │
//! │                          │                                              │
//! │    ┌─────────────────────┼─────────────────────┐                        │
//! │    ▼                     ▼                     ▼                        │
//! │ ┌──────────┐      ┌─────────────┐      ┌─────────────┐                  │
//! │ │  MySQL   │ ───► │  Transform  │ ───► │Elasticsearch│                  │
//! │ │  Query   │      │   Process   │      │   Index     │                  │
//! │ └──────────┘      └─────────────┘      └─────────────┘                  │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Parallel Execution Model
//!
//! Each batch schedule runs independently based on its cron expression:
//! - Schedules are registered with [`JobScheduler`] on startup
//! - Each job triggers `process_index_batch` for its specific index
//! - Multiple indices can be processed concurrently without blocking each other

use crate::{app_config::*, batch_log, common::*};

use crate::models::{ConsumingIndexProdtType, SpentDetail, batch_schedule::*};

use crate::service_trait::{
    batch_service::*, consume_service::ConsumeService, elastic_service::*, indexing_service::*,
    mysql_service::*, producer_service::ProducerService, public_data_service::PublicDataService,
};

/// Concrete implementation of the batch processing service.
///
/// `BatchServiceImpl` orchestrates batch operations by:
/// 1. Loading schedule configurations from TOML
/// 2. Registering cron jobs for each enabled schedule
/// 3. Processing indices in parallel based on their schedules
/// 4. Coordinating MySQL → Transform → Elasticsearch pipeline
///
/// # Type Parameters
///
/// * `M` - MySQL service implementation
/// * `E` - Elasticsearch service implementation
/// * `C` - Message consumption service implementation
///
/// # Thread Safety
///
/// The service is wrapped in `Arc` internally to allow safe sharing
/// across multiple async tasks spawned by the scheduler.
///
/// # Examples
///
/// ```rust,no_run
/// use crate::service::BatchServiceImpl;
///
/// let batch_service = BatchServiceImpl::new(
///     mysql_service,
///     elastic_service,
///     consume_service,
/// )?;
///
/// // Start the scheduler (runs indefinitely)
/// batch_service.main_batch_task().await?;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct BatchServiceImpl<M, E, C, P, D, I>
where
    M: MysqlService,
    E: ElasticService,
    C: ConsumeService,
    P: ProducerService,
    D: PublicDataService,
    I: IndexingService,
{
    /// Service for MySQL database operations.
    mysql_service: Arc<M>,

    /// Service for Elasticsearch operations.
    elastic_service: Arc<E>,

    /// Service for message consumption/processing.
    consume_service: Arc<C>,

    /// Loaded batch schedule configuration.
    schedule_config: BatchScheduleConfig,

    producer_service: Arc<P>,

    /// Service for fetching public data (e.g. Korean holidays).
    public_data_service: Arc<D>,

    indexing_service: Arc<I>,
}

// Manual Clone impl: Arc<T> is always Clone regardless of T: Clone
impl<M, E, C, P, D, I> Clone for BatchServiceImpl<M, E, C, P, D, I>
where
    M: MysqlService,
    E: ElasticService,
    C: ConsumeService,
    P: ProducerService,
    D: PublicDataService,
    I: IndexingService,
{
    /// Clones the batch service by cloning its shared dependency handles.
    fn clone(&self) -> Self {
        Self {
            mysql_service: Arc::clone(&self.mysql_service),
            elastic_service: Arc::clone(&self.elastic_service),
            consume_service: Arc::clone(&self.consume_service),
            schedule_config: self.schedule_config.clone(),
            producer_service: Arc::clone(&self.producer_service),
            public_data_service: Arc::clone(&self.public_data_service),
            indexing_service: Arc::clone(&self.indexing_service),
        }
    }
}

impl<M, E, C, P, D, I> BatchServiceImpl<M, E, C, P, D, I>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
{
    /// Creates a new `BatchServiceImpl` instance.
    ///
    /// Initializes the service by injecting dependencies and loading the
    /// batch schedule configuration from the environment-configured path.
    ///
    /// # Arguments
    ///
    /// * `mysql_service` - Implementation of [`MysqlService`] for database access
    /// * `elastic_service` - Implementation of [`ElasticService`] for search indexing
    /// * `consume_service` - Implementation of [`ConsumeService`] for message processing
    ///
    /// # Returns
    ///
    /// Returns `Ok(BatchServiceImpl)` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The batch schedule TOML file cannot be found
    /// - The configuration file contains invalid TOML syntax
    /// - Required configuration fields are missing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// let service = BatchServiceImpl::new(
    ///     mysql_service,
    ///     elastic_service,
    ///     consume_service,
    /// )?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn new(
        mysql_service: Arc<M>,
        elastic_service: Arc<E>,
        consume_service: Arc<C>,
        producer_service: Arc<P>,
        public_data_service: D,
        indexing_service: I,
    ) -> Result<Self> {
        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!("[BatchServiceImpl::new] app_config: {:#}", e);
        })?;
        let batch_schedule: &str = app_config.batch_schedule().as_str();

        let schedule_config: BatchScheduleConfig =
            BatchScheduleConfig::find_from_file(batch_schedule).inspect_err(|e| {
                error!("[BatchServiceImpl::new] schedule_config: {:#}", e);
            })?;

        batch_log!(
            info,
            "Loaded {} batch schedules ({} enabled)",
            schedule_config.batch_schedule().len(),
            schedule_config.find_enabled_schedules().len()
        );

        Ok(Self {
            mysql_service,
            elastic_service,
            consume_service,
            schedule_config,
            producer_service,
            public_data_service: Arc::new(public_data_service),
            indexing_service: Arc::new(indexing_service),
        })
    }

    /// Returns references to all enabled batch schedules.
    ///
    /// Filters schedules where `enabled` is `true`.
    ///
    /// # Returns
    ///
    /// A vector of references to enabled [`BatchScheduleItem`]s.
    pub fn find_enabled_schedules(&self) -> Vec<&BatchScheduleItem> {
        self.schedule_config.find_enabled_schedules()
    }

    /// Registers all enabled cron-based jobs into a new [`JobScheduler`] and starts it.
    ///
    /// Only schedules where `cron_schedule_apply` is `true` are registered here.
    /// Immediate (one-time) jobs are handled separately by [`spawn_immediate_jobs`].
    ///
    /// # Returns
    ///
    /// Returns the started [`JobScheduler`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The scheduler cannot be created
    /// - Any cron expression is invalid
    /// - Jobs cannot be added to the scheduler
    async fn initialize_cron_scheduler(&self) -> anyhow::Result<JobScheduler> {
        let scheduler: JobScheduler = JobScheduler::new()
            .await
            .inspect_err(|e| {
                error!("[BatchServiceImpl::initialize_cron_scheduler] Failed to create JobScheduler: {:#}", e);
            })?;

        let cron_schedules: Vec<&BatchScheduleItem> = self
            .find_enabled_schedules()
            .into_iter()
            .filter(|item| *item.cron_schedule_apply())
            .collect();

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_cron_scheduler] Registering {} cron job(s)",
            cron_schedules.len()
        );

        for schedule_item in cron_schedules {
            let job: Job = self.initialize_index_job(schedule_item)?;
            scheduler.add(job).await?;

            batch_log!(
                info,
                "[BatchServiceImpl::initialize_cron_scheduler] {} Cron job registered (schedule: {})",
                schedule_item.index_name(),
                schedule_item.cron_schedule()
            );
        }

        scheduler.start().await?;

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_cron_scheduler] Cron scheduler started successfully"
        );

        Ok(scheduler)
    }

    /// Spawns all enabled immediate (one-time) batch jobs and returns their handles.
    ///
    /// Only schedules where `immediate_apply` is `true` are spawned here.
    /// Cron-based jobs are handled separately by [`build_and_start_cron_scheduler`].
    ///
    /// # Returns
    ///
    /// Returns a [`JoinSet`] containing handles to all spawned immediate jobs.
    fn initialize_immediate_jobs(&self) -> JoinSet<()> {
        let mut immediate_jobs: JoinSet<()> = JoinSet::new();

        let immediate_schedules: Vec<&BatchScheduleItem> = self
            .find_enabled_schedules()
            .into_iter()
            .filter(|item| *item.immediate_apply())
            .collect();

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_immediate_jobs] Spawning {} immediate job(s)",
            immediate_schedules.len()
        );

        for immediate_item in immediate_schedules {
            let mysql: Arc<M> = Arc::clone(&self.mysql_service);
            let elastic: Arc<E> = Arc::clone(&self.elastic_service);
            let public_data: Arc<D> = Arc::clone(&self.public_data_service);
            let indexing: Arc<I> = Arc::clone(&self.indexing_service);
            let item: BatchScheduleItem = immediate_item.clone();

            batch_log!(
                info,
                "[BatchServiceImpl::initialize_immediate_jobs] {} Spawning immediate job (one-time execution)",
                immediate_item.index_name()
            );

            immediate_jobs.spawn(async move {
                batch_log!(info, "[{}] Immediate job started", item.index_name());

                match Self::input_batch_by_schedule(&item, &mysql, &elastic, &public_data, &indexing).await {
                    Ok(()) => {
                        batch_log!(
                            info,
                            "[{}] Immediate job completed successfully",
                            item.index_name()
                        );
                    }
                    Err(e) => {
                        batch_log!(error, "[{}] Immediate job failed: {}", item.index_name(), e);
                    }
                }
            });
        }

        immediate_jobs
    }

    /// Creates a scheduled job for a specific index.
    ///
    /// Wraps the indexing logic in a cron job that will be managed by
    /// the [`JobScheduler`].
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The schedule configuration for this index
    ///
    /// # Returns
    ///
    /// Returns a configured [`Job`] ready to be added to the scheduler.
    ///
    /// # Errors
    ///
    /// Returns an error if the cron expression is invalid.
    fn initialize_index_job(&self, schedule_item: &BatchScheduleItem) -> Result<Job> {
        let index_name: String = schedule_item.index_name().clone();
        let cron_expr: String = schedule_item.cron_schedule().clone();

        // Clone Arc references for the async closure
        // These clones are necessary because the closure must be 'static
        let mysql_service: Arc<M> = Arc::clone(&self.mysql_service);
        let elastic_service: Arc<E> = Arc::clone(&self.elastic_service);
        let public_data_service: Arc<D> = Arc::clone(&self.public_data_service);
        let indexing_service: Arc<I> = Arc::clone(&self.indexing_service);
        let schedule_item_move: BatchScheduleItem = schedule_item.clone();

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_index_job] {} Registering cron job with schedule: {}",
            index_name,
            cron_expr
        );

        // Process multiple tasks in parallel
        let job: Job = Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
            // Clone again for each job execution (closure is FnMut, called multiple times)
            let mysql: Arc<M> = Arc::clone(&mysql_service);
            let elastic: Arc<E> = Arc::clone(&elastic_service);
            let public_data: Arc<D> = Arc::clone(&public_data_service);
            let indexing: Arc<I> = Arc::clone(&indexing_service);
            let schedule_item: BatchScheduleItem = schedule_item_move.clone();

            Box::pin(async move {
                batch_log!(info, "[{}] Cron job triggered", schedule_item.index_name());

                // Call the batch processing logic with schedule_item
                match Self::input_batch_by_schedule(&schedule_item, &mysql, &elastic, &public_data, &indexing)
                    .await
                {
                    Ok(()) => {
                        batch_log!(
                            info,
                            "[{}] Cron job completed successfully",
                            schedule_item.index_name()
                        );
                    }
                    Err(e) => {
                        batch_log!(
                            error,
                            "[{}] Batch processing failed: {}",
                            schedule_item.index_name(),
                            e
                        );
                    }
                }
            })
        })?;

        Ok(job)
    }

    /// Processes a batch indexing job for a specific index.
    ///
    /// This is the core indexing logic that runs for each scheduled job:
    /// 1. Query data from MySQL
    /// 2. Transform/process the data
    /// 3. Bulk index to Elasticsearch
    /// 4. Post-process via consume service
    ///
    /// # Arguments
    ///
    /// * `batch_name` - ???
    /// * `mysql_service` - Arc reference to MySQL service
    /// * `elastic_service` - Arc reference to Elasticsearch service
    /// * `consume_service` - Arc reference to consume service
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if any step in the pipeline fails.
    ///
    /// # Note
    ///
    /// This is an associated function (not `&self` method) to allow calling
    /// from within `'static` closures used by the cron scheduler.
    async fn input_batch_by_schedule(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
        public_data_service: &Arc<D>,
        indexing_service: &Arc<I>,
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
                        error!(
                            "[BatchServiceImpl::input_batch_by_schedule] spent_detail_incremental: {:#}",
                            e
                        );
                    })?;
            }
            "spent_type" => {
                indexing_service
                    .input_spent_type_full(schedule_item)
                    .await
                    .inspect_err(|e| {
                        error!("[BatchServiceImpl::input_batch_by_schedule] spent_type: {:#}", e);
                    })?;
            }
            "all_change_spent_detail_type" => {
                Self::modify_all_spent_detail_type(
                    schedule_item,
                    mysql_service,
                    elastic_service,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] all_change_spent_detail_type: {:#}",
                        e
                    );
                })?;
            }
            "dimension_date_table" => {
                Self::input_date_dimension_data(
                    schedule_item,
                    mysql_service,
                    public_data_service,
                )
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::input_batch_by_schedule] dimension_date_table: {:#}",
                        e
                    );
                })?;
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
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
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
                    {
                       spent_idx: 152,
                       spent_name: "네이버페이",
                       ...
                    }, -> D1
                    {
                       spent_idx: 153,
                       spent_name: "쿠팡",
                       ...
                    }, -> D2
                    {
                       spent_idx: 157,
                       spent_name: "카카오",
                       ...
                    } -> D3
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
                            let delay_secs: u64 = 2u64.pow(attempt - 1); // 1s, 2s, 4s
                            batch_log!(
                                error,
                                "[BatchServiceImpl::modify_all_spent_detail_type] fetch failed (attempt {}/{}, offset={}, retry in {}s): {:#}",
                                attempt, MAX_RETRIES, offset, delay_secs, e
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
                            MAX_RETRIES, offset, last_err.unwrap()
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

            let spent_names: Vec<String> = details
                .iter()
                .map(|detail| detail.spent_name().clone())
                .collect();

            /*
                spent_types = [
                    {
                        "consume_keyword": "네이버페이",
                        "consume_keyword_type": "인터넷 쇼핑",
                        "consume_keyword_type_id": 16,
                        "keyword_weight": 1
                    }, -> S1
                    {
                        "consume_keyword": "쿠팡",
                        "consume_keyword_type": "인터넷 쇼핑",
                        "consume_keyword_type_id": 16,
                        "keyword_weight": 1
                    }, -> S2
                    {
                        "consume_keyword": "카카오",
                        "consume_keyword_type": "인터넷 쇼핑",
                        "consume_keyword_type_id": 16,
                        "keyword_weight": 1
                    } -> S3
                ]
            */
            let spent_types: Vec<ConsumingIndexProdtType> = elastic_service
                .find_consume_type_judgements(&spent_names)
                .await
                .inspect_err(|e| {
                    error!("[BatchServiceImpl::modify_all_spent_detail_type] spent_types: {:#}", e);
                })?;

            if spent_types.len() != details.len() {
                return Err(anyhow!(
                    "[BatchServiceImpl::modify_all_spent_detail_type] spent_types length mismatch. details={}, spent_types={}",
                    details.len(),
                    spent_types.len()
                ));
            }
            
            /*
                ## Find records that need type update ##
                updates = [
                    (D1.spent_idx, S1.consume_keyword_type_id),
                    (D2.spent_idx, S2.consume_keyword_type_id),
                    (D3.spent_idx, S3.consume_keyword_type_id)
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

    /// Populates DIM_CALENDAR for every day in [start_year, end_year].
    ///
    /// Iterates day by day, builds a `dim_calendar::ActiveModel` for each date,
    /// then bulk-inserts in chunks of `batch_size`. Duplicate dates (dt PK) are
    /// overwritten with the latest data, so the function is safe to re-run.
    ///
    /// When `PUBLIC_DATA_API_KEY` is set in the environment, Korean legal holidays
    /// are fetched from the 공공데이터포털 API and used to populate
    /// `is_holiday`, `is_before_holiday`, and `is_after_holiday`.
    /// If the key is absent or the API call fails, all three fields default to `0`.
    async fn input_date_dimension_data(
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

        /// Returns the number of days in the given month.
        fn days_in_month(year: i32, month: u32) -> anyhow::Result<u32> {
            let first_of_next: NaiveDate = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1)
            }
            .ok_or_else(|| anyhow!("Invalid date: year={} month={}", year, month))?;

            let last: NaiveDate = first_of_next
                .pred_opt() // previous day
                .ok_or_else(|| anyhow!("Date underflow at {:?}", first_of_next))?;

            Ok(last.day())
        }

        // Fetch Korean holidays for the full year range via the injected service.
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

            // weekday_no: 0=Mon ~ 6=Sun (ISO weekday - 1)
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
                        error!(
                            "[BatchServiceImpl::input_date_dimension_data] Bulk insert failed: {:#}",
                            e
                        );
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
                    error!(
                        "[BatchServiceImpl::input_date_dimension_data] Final bulk insert failed: {:#}",
                        e
                    );
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

#[async_trait]
impl<M, E, C, P, D, I> BatchService for BatchServiceImpl<M, E, C, P, D, I>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
{
    /// Main entry point for the batch processing service.
    ///
    /// Starts the cron scheduler and keeps it running indefinitely.
    /// Each enabled schedule will trigger its indexing job based on
    /// the configured cron expression.
    ///
    /// # Execution Model
    ///
    /// ```text
    /// main_batch_task()
    ///        │
    ///        ▼
    /// ┌──────────────┐
    /// │ JobScheduler │ ◄─── runs indefinitely
    /// └──────────────┘
    ///        │
    ///        ├──► Job[index_1] ──► process_index_batch("index_1")
    ///        ├──► Job[index_2] ──► process_index_batch("index_2")
    ///        └──► Job[index_3] ──► process_index_batch("index_3")
    ///                  │
    ///                  └──► Each job runs independently on its cron schedule
    /// ```
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` only if the scheduler is shut down gracefully.
    /// Waits for shutdown signal (Ctrl+C) and gracefully stops the scheduler.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduler fails to start or shutdown.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// let service = BatchServiceImpl::new(mysql, elastic, consume)?;
    ///
    /// // This will block until Ctrl+C is received
    /// service.main_batch_task().await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    async fn initialize_batch_task(&self) -> anyhow::Result<()> {
        batch_log!(
            info,
            "[BatchServiceImpl::initialize_batch_task] Starting batch service main task"
        );

        /* 1. Start scheduler tasks */
        let mut scheduler: JobScheduler = self.initialize_cron_scheduler().await?;
        /* 2. Start immediate job tasks */
        let mut immediate_jobs: JoinSet<()> = self.initialize_immediate_jobs();

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_batch_task] Scheduler is running. Press Ctrl+C to shutdown gracefully."
        );

        // Wait for either:
        // 1. Shutdown signal (Ctrl+C)
        // 2. All immediate jobs complete -> then keep alive until Ctrl+C
        // 3. *** tokio::select! chooses the branch that "completes first", not the branch that starts "executing first" ***
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                batch_log!(info,
                    "[BatchServiceImpl::initialize_batch_task] Shutdown signal received, stopping scheduler..."
                );
            }
            _ = async {
                // Wait for all immediate jobs to complete
                while let Some(result) = immediate_jobs.join_next().await {
                    if let Err(e) = result {
                        batch_log!(error,"[BatchServiceImpl::initialize_batch_task] Immediate job panicked: {:?}", e);
                    }
                }

                batch_log!(info,"[BatchServiceImpl::initialize_batch_task] All immediate jobs completed, keeping service alive until Ctrl+C...");

                // Always keep running until Ctrl+C (socket server + cron jobs may still be active)
                // pending -> std::future::pending returns a `Future` that never resolves to `Ready`.
                //         -> Never complete.
                std::future::pending::<()>().await;
            } => {}
        }

        // Gracefully shutdown the scheduler
        scheduler.shutdown().await.inspect_err(|e| {
            error!(
                "[BatchServiceImpl::initialize_batch_task] Failed to shutdown scheduler: {:#}",
                e
            );
        })?;

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_batch_task] Scheduler stopped gracefully. Goodbye!"
        );

        Ok(())
    }

    /// Runs a single batch schedule immediately.
    ///
    /// Delegates directly to `process_batch` using the current service's dependencies.
    /// Used by the CLI service to trigger on-demand batch execution via socket.
    ///
    /// # Arguments
    ///
    /// * `schedule_item` - The batch schedule item to execute
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful completion.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying batch processing fails.
    async fn input_batch(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        Self::input_batch_by_schedule(
            schedule_item,
            &self.mysql_service,
            &self.elastic_service,
            &self.public_data_service,
            &self.indexing_service,
        )
        .await
    }
}
