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
//! │  │  (cron)     │  │  (cron)     │  │  (cron)     │                      │
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

use crate::app_config::*;
use crate::common::*;

use crate::models::{batch_schedule::*, spent_type_keyword::*};
use crate::service_trait::producer_service;
use crate::service_trait::{
    batch_service::*, consume_service::*, elastic_service::*, mysql_service::*, producer_service::*,
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
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct BatchServiceImpl<M, E, C, P>
where
    M: MysqlService,
    E: ElasticService,
    C: ConsumeService,
    P: ProducerService,
{
    /// Service for MySQL database operations.
    mysql_service: Arc<M>,

    /// Service for Elasticsearch operations.
    elastic_service: Arc<E>,

    /// Service for message consumption/processing.
    consume_service: Arc<C>,

    /// Loaded batch schedule configuration.
    schedule_config: BatchScheduleConfig,

    produce_service: Arc<P>,
}

impl<M, E, C, P> BatchServiceImpl<M, E, C, P>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
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
        mysql_service: M,
        elastic_service: E,
        consume_service: C,
        produce_service: P,
    ) -> Result<Self> {
        let app_config: &AppConfig = AppConfig::global();
        let batch_schedule: &str = app_config.batch_schedule().as_str();

        let schedule_config: BatchScheduleConfig =
            BatchScheduleConfig::load_from_file(batch_schedule)
                .context("[BatchServiceImpl::new] schedule_config: ")?;

        info!(
            "Loaded {} batch schedules ({} enabled)",
            schedule_config.batch_schedule().len(),
            schedule_config.get_enabled_schedules().len()
        );

        Ok(Self {
            mysql_service: Arc::new(mysql_service),
            elastic_service: Arc::new(elastic_service),
            consume_service: Arc::new(consume_service),
            schedule_config,
            produce_service: Arc::new(produce_service),
        })
    }

    /// Returns references to all enabled batch schedules.
    ///
    /// Filters schedules where `enabled` is `true`.
    ///
    /// # Returns
    ///
    /// A vector of references to enabled [`BatchScheduleItem`]s.
    pub fn get_enabled_schedules(&self) -> Vec<&BatchScheduleItem> {
        self.schedule_config.get_enabled_schedules()
    }

    /// Initializes and starts the batch job scheduler.
    ///
    /// Creates a [`JobScheduler`], registers all enabled schedules based on
    /// their `cron_schedule_apply` setting:
    /// - `true`: Register with cron scheduler (periodic execution)
    /// - `false`: Run once immediately (one-time execution)
    ///
    /// # Returns
    ///
    /// Returns the running [`JobScheduler`] instance and a JoinSet for immediate jobs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The scheduler cannot be created
    /// - Any cron expression is invalid
    /// - Jobs cannot be added to the scheduler
    async fn start_scheduler(&self) -> anyhow::Result<(JobScheduler, JoinSet<()>)> {
        let scheduler: JobScheduler = JobScheduler::new()
            .await
            .context("[BatchServiceImpl::start_scheduler] scheduler: ")?;

        let mut immediate_jobs: JoinSet<()> = JoinSet::new();

        let enabled_schedules: Vec<&BatchScheduleItem> = self.get_enabled_schedules();

        if enabled_schedules.is_empty() {
            error!("[BatchServiceImpl::start_scheduler] No enabled batch schedules found");
            return Ok((scheduler, immediate_jobs));
        }

        // Separate schedules by cron_schedule_apply
        let (cron_schedules, immediate_schedules): (Vec<_>, Vec<_>) = enabled_schedules
            .into_iter()
            .partition(|item| *item.cron_schedule_apply());

        info!(
            "[BatchServiceImpl::start_scheduler] Found {} cron jobs, {} immediate jobs",
            cron_schedules.len(),
            immediate_schedules.len()
        );

        // Register cron-based jobs with scheduler
        for schedule_item in cron_schedules {
            let job: Job = self.create_index_job(schedule_item)?;
            scheduler.add(job).await?;

            info!(
                "[BatchServiceImpl::start_scheduler] {} Cron job registered (schedule: {})",
                schedule_item.index_name(),
                schedule_item.cron_schedule()
            );
        }

        // Spawn immediate (one-time) jobs
        for schedule_item in immediate_schedules {
            let mysql: Arc<M> = Arc::clone(&self.mysql_service);
            let elastic: Arc<E> = Arc::clone(&self.elastic_service);
            let consume: Arc<C> = Arc::clone(&self.consume_service);
            let item: BatchScheduleItem = schedule_item.clone();

            info!(
                "[BatchServiceImpl::start_scheduler] {} Spawning immediate job (one-time execution)",
                schedule_item.index_name()
            );

            immediate_jobs.spawn(async move {
                info!("[{}] Immediate job started", item.index_name());

                match Self::process_index_batch(&item, &mysql, &elastic, &consume).await {
                    Ok(()) => {
                        info!(
                            "[{}] Immediate job completed successfully",
                            item.index_name()
                        );
                    }
                    Err(e) => {
                        error!("[{}] Immediate job failed: {}", item.index_name(), e);
                    }
                }
            });
        }

        scheduler.start().await?;

        info!("[BatchServiceImpl::start_scheduler] Batch scheduler started successfully");

        Ok((scheduler, immediate_jobs))
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
    fn create_index_job(&self, schedule_item: &BatchScheduleItem) -> Result<Job> {
        let index_name: String = schedule_item.index_name().clone();
        let cron_expr: String = schedule_item.cron_schedule().clone();

        // Clone Arc references for the async closure
        // These clones are necessary because the closure must be 'static
        let mysql_service: Arc<M> = Arc::clone(&self.mysql_service);
        let elastic_service: Arc<E> = Arc::clone(&self.elastic_service);
        let consume_service: Arc<C> = Arc::clone(&self.consume_service);
        let schedule_item_move: BatchScheduleItem = schedule_item.clone();

        info!(
            "[BatchServiceImpl::create_index_job] {} Registering cron job with schedule: {}",
            index_name, cron_expr
        );

        let job: Job = Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
            // Clone again for each job execution (closure is FnMut, called multiple times)
            let mysql: Arc<M> = Arc::clone(&mysql_service);
            let elastic: Arc<E> = Arc::clone(&elastic_service);
            let consume: Arc<C> = Arc::clone(&consume_service);
            let schedule_item: BatchScheduleItem = schedule_item_move.clone();

            Box::pin(async move {
                info!("[{}] Cron job triggered", schedule_item.index_name());

                // Call the batch processing logic with schedule_item
                match Self::process_index_batch(&schedule_item, &mysql, &elastic, &consume).await {
                    Ok(()) => {
                        info!(
                            "[{}] Cron job completed successfully",
                            schedule_item.index_name()
                        );
                    }
                    Err(e) => {
                        error!(
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
    async fn process_index_batch(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
        consume_service: &Arc<C>,
    ) -> Result<()> {
        let start_time: DateTime<Utc> = Utc::now();
        let batch_name: &str = schedule_item.batch_name();
        let index_name: &str = schedule_item.index_name();

        info!(
            "[BatchServiceImpl::process_index_batch] {} Starting batch indexing job",
            index_name
        );

        // Route to specific handler based on batch_name
        match batch_name {
            "spent_detail_full" => {
                Self::process_spent_detail_full(
                    schedule_item,
                    mysql_service,
                    elastic_service,
                    consume_service,
                )
                .await?;
            }
            "spent_type_dev" | "spent_type_full" => {
                Self::process_spent_type_full(schedule_item, mysql_service, elastic_service)
                    .await?;
            }
            // "spent_detail_migration_to_kafka" => {

            // }
            // "spent_detail2" => {
            //     Self::process_spent_detail2(schedule_item, mysql_service, elastic_service, consume_service).await?;
            // }
            // "spent_detail3" => {
            //     Self::process_spent_detail3(schedule_item, mysql_service, elastic_service, consume_service).await?;
            // }
            _ => {
                warn!(
                    "[BatchServiceImpl::process_index_batch] Unknown batch_name: {}, skipping",
                    batch_name
                );
            }
        }

        let elapsed: chrono::TimeDelta = Utc::now() - start_time;

        info!(
            "[BatchServiceImpl::process_index_batch] {} Batch indexing completed successfully in {}ms",
            index_name,
            elapsed.num_milliseconds()
        );

        Ok(())
    }

    // ============================================================================
    // Index-specific handler functions
    // ============================================================================
    async fn process_spent_detail_full(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
        consume_service: &Arc<C>,
    ) -> anyhow::Result<()> {
        info!(
            "[BatchServiceImpl::process_spent_detail] Processing {} (index: {})",
            schedule_item.batch_name(),
            schedule_item.index_name()
        );

        let relation_topic: &str = schedule_item.relation_topic();
        let index_alias: &str = schedule_item.index_name();
        let batch_size: usize = *schedule_item.batch_size();
        // TODO: Implement spent_detail processing logic
        // 1. Query data from MySQL using mysql_service
        // 2. Transform data as needed
        // 3. Index to Elasticsearch using elastic_service
        // 4. Post-process using consume_service if needed

        //consume_service.consume_messages(topic, max_messages)
        loop {
            // let consume_datas: Vec<Value> = consume_service
            //     .consume_messages(relation_topic, batch_size)
            //     .await
            //     .context("[BatchServiceImpl::process_spent_detail_full]")?;

            //info!("consume_datas: {:?}", consume_datas);

            tokio::time::sleep(tokio_duration::from_secs(10)).await;
        }

        info!("[BatchServiceImpl::process_spent_detail] Completed");
        Ok(())
    }

    // async fn process_migration_spent_detail_to_kafka(
    //     schedule_item: &BatchScheduleItem,
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    //     kafka_service: &Arc<>
    // )

    async fn process_spent_type_full(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        info!(
            "[BatchServiceImpl::process_spent_type_full] Processing {} (index: {})",
            schedule_item.batch_name(),
            schedule_item.index_name()
        );

        let index_alias: &str = schedule_item.index_name();
        let batch_size: usize = *schedule_item.batch_size();
        let mapping_schema_path: &str = schedule_item.mapping_schema();

        // Step 1: Load mapping schema from file
        let schema_content: String = tokio::fs::read_to_string(mapping_schema_path)
            .await
            .context(format!(
                "[BatchServiceImpl::process_spent_type_full] Failed to read mapping schema file: {}",
                mapping_schema_path
            ))?;

        let schema: Value = serde_json::from_str(&schema_content).context(
            "[BatchServiceImpl::process_spent_type_full] Failed to parse mapping schema",
        )?;

        let mappings: Value = schema
            .get("mappings")
            .ok_or_else(|| {
                anyhow!("[BatchServiceImpl::process_spent_type_full] Missing 'mappings' in schema")
            })?
            .clone();

        // Get settings from schema file (includes analyzer definitions)
        let mut schema_settings: Value =
            schema.get("settings").cloned().unwrap_or_else(|| json!({}));

        // Merge initial indexing settings for fast bulk indexing
        if let Some(index_obj) = schema_settings.get_mut("index") {
            if let Some(obj) = index_obj.as_object_mut() {
                obj.insert("number_of_shards".to_string(), json!(3));
                obj.insert("number_of_replicas".to_string(), json!(0));
                obj.insert("refresh_interval".to_string(), json!("-1"));
            }
        } else {
            schema_settings["index"]["number_of_shards"] = json!(3);
            schema_settings["index"]["number_of_replicas"] = json!(0);
            schema_settings["index"]["refresh_interval"] = json!("-1");
        }

        // Step 2: Create new index with timestamp
        let now: DateTime<Utc> = Utc::now();
        let new_index_name: String = format!("{}_{}", index_alias, now.format("%Y%m%d%H%M%S"));

        info!(
            "[BatchServiceImpl::process_spent_type_full] Creating new index: {}",
            new_index_name
        );

        let initial_settings: Value = schema_settings;

        elastic_service
            .create_index(&new_index_name, &initial_settings, &mappings)
            .await
            .context("[BatchServiceImpl::process_spent_type_full] Failed to create index")?;

        // Step 3: Batch index data from MySQL
        let mut offset: u64 = 0;
        let mut total_indexed: u64 = 0;

        loop {
            info!(
                "[BatchServiceImpl::process_spent_type_full] Fetching batch at offset: {}, batch_size: {}",
                offset, batch_size
            );

            let keywords: Vec<SpentTypeKeyword> = mysql_service
                .fetch_spent_type_keywords_batch(offset, batch_size as u64)
                .await
                .context(
                    "[BatchServiceImpl::process_spent_type_full] Failed to fetch keywords batch",
                )?;

            if keywords.is_empty() {
                info!("[BatchServiceImpl::process_spent_type_full] No more data to index");
                break;
            }

            let batch_count: usize = keywords.len();
            info!(
                "[BatchServiceImpl::process_spent_type_full] Indexing {} documents",
                batch_count
            );

            elastic_service
                .bulk_index(&new_index_name, keywords)
                .await
                .context("[BatchServiceImpl::process_spent_type_full] Failed to bulk index")?;

            total_indexed += batch_count as u64;
            offset += batch_size as u64;

            info!(
                "[BatchServiceImpl::process_spent_type_full] Indexed {} documents so far",
                total_indexed
            );
        }

        // Step 4: Update index settings to production values
        info!("[BatchServiceImpl::process_spent_type_full] Updating index settings for production");

        let production_settings: Value = json!({
            "index": {
                "number_of_replicas": 1,
                "refresh_interval": "1s"
            }
        });

        elastic_service
            .update_index_settings(&new_index_name, &production_settings)
            .await
            .context(
                "[BatchServiceImpl::process_spent_type_full] Failed to update index settings",
            )?;

        // Step 5: Swap alias to new index
        info!(
            "[BatchServiceImpl::process_spent_type_full] Swapping alias {} to new index {}",
            index_alias, new_index_name
        );

        elastic_service
            .swap_alias(index_alias, &new_index_name)
            .await
            .context("[BatchServiceImpl::process_spent_type_full] Failed to swap alias")?;

        info!(
            "[BatchServiceImpl::process_spent_type_full] Completed successfully. Total indexed: {}",
            total_indexed
        );

        Ok(())
    }

    // /// Processes batch job for `spent_detail_dev` index.
    // ///
    // /// # Arguments
    // ///
    // /// * `mysql_service` - MySQL service for data retrieval
    // /// * `elastic_service` - Elasticsearch service for indexing
    // /// * `consume_service` - Consume service for message processing
    // ///
    // /// # Returns
    // ///
    // /// Returns `Ok(())` on successful processing.
    // async fn process_spent_detail_dev(
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    //     consume_service: &Arc<C>,
    // ) -> Result<()> {
    //     info!("[BatchServiceImpl::process_spent_detail_dev] Processing spent_detail_dev index");

    //     // TODO: Implement spent_detail_dev processing logic
    //     // 1. Query data from MySQL using mysql_service
    //     // 2. Transform data as needed
    //     // 3. Index to Elasticsearch using elastic_service
    //     // 4. Post-process using consume_service if needed

    //     info!("[BatchServiceImpl::process_spent_detail_dev] Completed");
    //     Ok(())
    // }

    // /// Processes batch job for `spent_detail_dev2` index.
    // ///
    // /// # Arguments
    // ///
    // /// * `mysql_service` - MySQL service for data retrieval
    // /// * `elastic_service` - Elasticsearch service for indexing
    // /// * `consume_service` - Consume service for message processing
    // ///
    // /// # Returns
    // ///
    // /// Returns `Ok(())` on successful processing.
    // async fn process_spent_detail_dev2(
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    //     consume_service: &Arc<C>,
    // ) -> Result<()> {
    //     info!("[BatchServiceImpl::process_spent_detail_dev2] Processing spent_detail_dev2 index");

    //     // TODO: Implement spent_detail_dev2 processing logic
    //     // 1. Query data from MySQL using mysql_service
    //     // 2. Transform data as needed
    //     // 3. Index to Elasticsearch using elastic_service
    //     // 4. Post-process using consume_service if needed

    //     info!("[BatchServiceImpl::process_spent_detail_dev2] Completed");
    //     Ok(())
    // }

    // /// Processes batch job for `spent_detail_dev3` index.
    // ///
    // /// # Arguments
    // ///
    // /// * `mysql_service` - MySQL service for data retrieval
    // /// * `elastic_service` - Elasticsearch service for indexing
    // /// * `consume_service` - Consume service for message processing
    // ///
    // /// # Returns
    // ///
    // /// Returns `Ok(())` on successful processing.
    // async fn process_spent_detail_dev3(
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    //     consume_service: &Arc<C>,
    // ) -> Result<()> {
    //     info!("[BatchServiceImpl::process_spent_detail_dev3] Processing spent_detail_dev3 index");

    //     // TODO: Implement spent_detail_dev3 processing logic
    //     // 1. Query data from MySQL using mysql_service
    //     // 2. Transform data as needed
    //     // 3. Index to Elasticsearch using elastic_service
    //     // 4. Post-process using consume_service if needed

    //     info!("[BatchServiceImpl::process_spent_detail_dev3] Completed");
    //     Ok(())
    // }
}

#[async_trait]
impl<M, E, C, P> BatchService for BatchServiceImpl<M, E, C, P>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
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
    async fn main_batch_task(&self) -> anyhow::Result<()> {
        info!("[BatchServiceImpl::main_batch_task] Starting batch service main task");

        let (mut scheduler, mut immediate_jobs) = self.start_scheduler().await?;

        info!(
            "[BatchServiceImpl::main_batch_task] Scheduler is running. Press Ctrl+C to shutdown gracefully."
        );

        // Wait for either:
        // 1. Shutdown signal (Ctrl+C)
        // 2. All immediate jobs complete (if no cron jobs are registered)
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!(
                    "[BatchServiceImpl::main_batch_task] Shutdown signal received, stopping scheduler..."
                );
            }
            _ = async {
                // Wait for all immediate jobs to complete
                while let Some(result) = immediate_jobs.join_next().await {
                    if let Err(e) = result {
                        error!("[BatchServiceImpl::main_batch_task] Immediate job panicked: {:?}", e);
                    }
                }
                // If there are no cron jobs, we can exit after immediate jobs complete
                if self.get_enabled_schedules().iter().all(|s| !s.cron_schedule_apply()) {
                    info!("[BatchServiceImpl::main_batch_task] All immediate jobs completed, no cron jobs to run");
                } else {
                    // Keep running if there are cron jobs
                    std::future::pending::<()>().await;
                }
            } => {}
        }

        // Gracefully shutdown the scheduler
        scheduler
            .shutdown()
            .await
            .context("[BatchServiceImpl::main_batch_task] Failed to shutdown scheduler")?;

        info!("[BatchServiceImpl::main_batch_task] Scheduler stopped gracefully. Goodbye!");

        Ok(())
    }
}
