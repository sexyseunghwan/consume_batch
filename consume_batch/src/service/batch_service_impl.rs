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

use crate::models::{
    ConsumingIndexProdtType, SpentDetail, SpentDetailWithRelations, SpentDetailWithRelationsEs,
    batch_schedule::*, spent_type_keyword::*,
};

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

    producer_service: Arc<P>,
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
        producer_service: P,
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
            producer_service: Arc::new(producer_service),
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
        // 스케쥴러
        let scheduler: JobScheduler = JobScheduler::new()
            .await
            .context("[BatchServiceImpl::start_scheduler] Failed to create JobScheduler")?;

        // 즉시 실행될 작업
        let mut immediate_jobs: JoinSet<()> = JoinSet::new();

        // 실행될 배치작업들 -> 스케쥴 + 즉시실행 작업 섞여있음. -> 실행 허용된 작업들만 가져옴
        let enabled_schedules: Vec<&BatchScheduleItem> = self.get_enabled_schedules();

        if enabled_schedules.is_empty() {
            error!("[BatchServiceImpl::start_scheduler] No enabled batch schedules found");
            return Ok((scheduler, immediate_jobs));
        }

        // Separate schedules by cron_schedule_apply
        // 스케쥴 작업 | 즉시 실행 작업 -> 두개로 나눠진다.
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
        for immediate_item in immediate_schedules {
            let mysql: Arc<M> = Arc::clone(&self.mysql_service);
            let elastic: Arc<E> = Arc::clone(&self.elastic_service);
            let consume: Arc<C> = Arc::clone(&self.consume_service);
            let produce: Arc<P> = Arc::clone(&self.producer_service);
            let item: BatchScheduleItem = immediate_item.clone();

            info!(
                "[BatchServiceImpl::start_scheduler] {} Spawning immediate job (one-time execution)",
                immediate_item.index_name()
            );

            immediate_jobs.spawn(async move {
                info!("[{}] Immediate job started", item.index_name());

                match Self::process_batch(&item, &mysql, &elastic, &consume, &produce).await {
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

        // 스케쥴러 작업은 여기서 실제적으로 시작해준다.
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
        let producer_service: Arc<P> = Arc::clone(&self.producer_service);
        let schedule_item_move: BatchScheduleItem = schedule_item.clone();

        info!(
            "[BatchServiceImpl::create_index_job] {} Registering cron job with schedule: {}",
            index_name, cron_expr
        );

        // Process multiple tasks in parallel
        let job: Job = Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
            // Clone again for each job execution (closure is FnMut, called multiple times)
            let mysql: Arc<M> = Arc::clone(&mysql_service);
            let elastic: Arc<E> = Arc::clone(&elastic_service);
            let consume: Arc<C> = Arc::clone(&consume_service);
            let produce: Arc<P> = Arc::clone(&producer_service);
            let schedule_item: BatchScheduleItem = schedule_item_move.clone();

            Box::pin(async move {
                info!("[{}] Cron job triggered", schedule_item.index_name());

                // Call the batch processing logic with schedule_item
                match Self::process_batch(&schedule_item, &mysql, &elastic, &consume, &produce)
                    .await
                {
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
    async fn process_batch(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
        consume_service: &Arc<C>,
        producer_service: &Arc<P>,
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
                Self::process_spent_detail_full(schedule_item, elastic_service, consume_service)
                    .await?;
            }
            "spent_type_dev" | "spent_type_full" => {
                Self::process_spent_type_full(schedule_item, mysql_service, elastic_service)
                    .await?;
            }
            "spent_detail_migration_to_kafka" => {
                Self::process_migration_spent_detail_to_kafka(schedule_item, mysql_service, producer_service)
                    .await
                    .context("[BatchServiceImpl::process_index_batch] process_migration_spent_detail_to_kafka ")?;
            }
            "all_change_spent_detail_type" => {
                Self::process_update_all_check_type_detail(
                    schedule_item,
                    mysql_service,
                    elastic_service,
                )
                .await
                .context("[BatchServiceImpl::process_index_batch] all_change_spent_detail_type ")?;
            }
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

    /* ====================================================================================================== */
    /* ====================================================================================================== */
    /* ====================================================================================================== */
    /* ====================================================================================================== */
    /* ====================================================================================================== */

    async fn process_update_all_check_type_detail(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;

        info!(
            "[BatchServiceImpl::process_update_all_check_type_detail] Starting. batch_size={}",
            batch_size
        );

        let mut offset: u64 = 0;
        let mut total_updated: u64 = 0;
        let mut total_processed: u64 = 0;

        loop {
            let details: Vec<SpentDetail> = mysql_service
                .fetch_spent_details(offset, batch_size)
                .await
                .context("[process_update_all_check_type_detail] Failed to fetch spent details")?;

            if details.is_empty() {
                break;
            }

            let batch_count: usize = details.len();
            total_processed += batch_count as u64;

            // Find records that need type update
            let mut updates: Vec<(i64, i64)> = Vec::new();

            for detail in &details {
                let spent_type: ConsumingIndexProdtType = elastic_service
                    .get_consume_type_judgement(detail.spent_name())
                    .await
                    .context(
                        "[BatchServiceImpl::process_update_all_check_type_detail] spent_type ",
                    )?;

                updates.push((*detail.spent_idx(), *spent_type.consume_keyword_type_id()));
            }

            if !updates.is_empty() {
                let update_count: usize = updates.len();
                let updated: u64 = mysql_service
                    //.update_spent_detail_type_one_by_one(updates)
                    .update_spent_detail_type_batch(updates)
                    .await
                    .context("[process_update_all_check_type_detail] Failed to update batch")?;

                total_updated += updated;

                info!(
                    "[process_update_all_check_type_detail] Batch offset={}, updated {}/{} records",
                    offset, update_count, batch_count
                );
            }

            offset += batch_size;
        }

        info!(
            "[process_update_all_check_type_detail] Completed. processed={}, updated={}",
            total_processed, total_updated
        );

        Ok(())
    }

    async fn process_migration_spent_detail_to_kafka(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        producer_service: &Arc<P>,
    ) -> anyhow::Result<()> {
        let produce_topic: &str = schedule_item.relation_topic().as_str();
        let batch_size: u64 = *schedule_item.batch_size() as u64;

        info!(
            "[BatchServiceImpl::process_migration_spent_detail_to_kafka] Starting. batch_size={}",
            batch_size
        );

        let mut offset: u64 = 0;
        let mut total_selected: usize = 0;

        // Delete all existing topic data.
        producer_service
            .purge_topic(produce_topic)
            .await
            .context("[BatchServiceImpl::process_migration_spent_detail_to_kafka] ")?;

        info!(
            "[BatchServiceImpl::process_migration_spent_detail_to_kafka] All data for the `{}` topic has been deleted.",
            produce_topic
        );

        loop {
            
            let produce_spent_details: Vec<SpentDetailWithRelations> = mysql_service
                .fetch_spent_details_for_indexing(offset, batch_size)
                .await
                .context("[BatchServiceImpl::process_migration_spent_detail_to_kafka] Failed to load vector for `produce_spent_details` from DB. ")?;

            if produce_spent_details.is_empty() {
                break;
            }

            for spent_detail in &produce_spent_details {
                let spent_detail_json: Value = serde_json::to_value(spent_detail)
                    .context("[BatchServiceImpl::process_migration_spent_detail_to_kafka] Failed to convert `spent_detail` to JSON")?;

                match producer_service
                    .produce_object_to_topic(produce_topic, &spent_detail_json, None)
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "[BatchServiceImpl::process_migration_spent_detail_to_kafka] Failed to produce data to Kafka. {:#}",
                            e
                        );
                        break;
                    }
                }
            }

            offset += batch_size;
            total_selected += produce_spent_details.len();
        }

        info!(
            "[BatchServiceImpl::process_migration_spent_detail_to_kafka] Successfully produced {} data points",
            total_selected
        );

        Ok(())
    }

    // ============================================================================
    // Common helper functions for full indexing
    // ============================================================================

    /// Prepares a new ES index for full indexing.
    ///
    /// 1. Loads mapping schema from file
    /// 2. Merges bulk indexing settings (shards=3, replicas=0, refresh=-1)
    /// 3. Creates a new timestamped index (e.g. "spent_detail_dev_20260213120000")
    ///
    /// Returns the created index name.
    async fn prepare_full_index(
        elastic_service: &Arc<E>,
        index_alias: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String> {
        // Load mapping schema from file
        let schema_content: String = tokio::fs::read_to_string(mapping_schema_path)
            .await
            .context(format!(
                "[BatchServiceImpl::prepare_full_index] Failed to read mapping schema file: {}",
                mapping_schema_path
            ))?;

        let schema: Value = serde_json::from_str(&schema_content)
            .context("[BatchServiceImpl::prepare_full_index] Failed to parse mapping schema")?;

        let mappings: Value = schema
            .get("mappings")
            .ok_or_else(|| {
                anyhow!("[BatchServiceImpl::prepare_full_index] Missing 'mappings' in schema")
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

        // Create new index with timestamp
        let now: DateTime<Utc> = Utc::now();
        let new_index_name: String = format!("{}_{}", index_alias, now.format("%Y%m%d%H%M%S"));

        info!(
            "[BatchServiceImpl::prepare_full_index] Creating new index: {}",
            new_index_name
        );

        elastic_service
            .create_index(&new_index_name, &schema_settings, &mappings)
            .await
            .context("[BatchServiceImpl::prepare_full_index] Failed to create index")?;

        Ok(new_index_name)
    }

    /// Finalizes a full index after bulk indexing is complete.
    ///
    /// 1. Updates index settings to production values (replicas=1, refresh=1s)
    /// 2. Swaps alias from old index to the new index
    async fn finalize_full_index(
        elastic_service: &Arc<E>,
        index_alias: &str,
        new_index_name: &str,
    ) -> anyhow::Result<()> {
        info!(
            "[BatchServiceImpl::finalize_full_index] Updating index settings for production: {}",
            new_index_name
        );

        let production_settings: Value = json!({
            "index": {
                "number_of_replicas": 1,
                "refresh_interval": "1s"
            }
        });

        elastic_service
            .update_index_settings(new_index_name, &production_settings)
            .await
            .context("[BatchServiceImpl::finalize_full_index] Failed to update index settings")?;

        info!(
            "[BatchServiceImpl::finalize_full_index] Swapping alias {} to new index {}",
            index_alias, new_index_name
        );

        elastic_service
            .swap_alias(index_alias, new_index_name)
            .await
            .context("[BatchServiceImpl::finalize_full_index] Failed to swap alias")?;

        Ok(())
    }

    // ============================================================================
    // Index-specific handler functions
    // ============================================================================
    async fn process_spent_detail_full(
        schedule_item: &BatchScheduleItem,
        elastic_service: &Arc<E>,
        consume_service: &Arc<C>,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let relation_topic: &str = schedule_item.relation_topic();
        let batch_size: usize = *schedule_item.batch_size();

        info!(
            "[BatchServiceImpl::process_spent_detail_full] Processing {} (index: {})",
            schedule_item.batch_name(),
            index_alias
        );
        
        // Step 1~2: 스키마 로드 + 벌크 인덱스 생성
        let new_index_name: String =
            Self::prepare_full_index(elastic_service, index_alias, schedule_item.mapping_schema())
                .await
                .context("[BatchServiceImpl::process_spent_detail_full] prepare_full_index")?;

        // Step 3: Kafka 토픽에서 데이터를 소비하여 ES에 색인
        // 풀색인이므로 토픽의 모든 메시지를 소비하면 종료한다.
        // consume_messages 는 5초 timeout 후 빈 벡터를 반환하므로,
        // 빈 벡터가 오면 토픽을 전부 소비한 것으로 판단하고 루프를 종료한다.
        let mut total_indexed: u64 = 0;

        loop {
            let messages: Vec<SpentDetailWithRelations> = consume_service
                .consume_messages_as(relation_topic, batch_size)
                .await
                .context(
                    "[BatchServiceImpl::process_spent_detail_full] Failed to consume messages",
                )?;
            
            if messages.is_empty() {
                info!(
                    "[BatchServiceImpl::process_spent_detail_full] No more messages in topic '{}', finishing",
                    relation_topic
                );
                break;
            }
            
            let batch_count: usize = messages.len();
            
            info!(
                "[BatchServiceImpl::process_spent_detail_full] Consumed {} messages, converting for ES indexing",
                batch_count
            );
            
            // Convert to ES-specific structure (excludes indexing_type field)
            let es_messages: Vec<SpentDetailWithRelationsEs> = messages
                .into_iter()
                .map(|msg| msg.into())
                .collect();

            info!(
                "[BatchServiceImpl::process_spent_detail_full] Indexing {} documents to {}",
                batch_count, new_index_name
            );

            elastic_service
                .bulk_index(&new_index_name, es_messages)
                .await
                .context("[BatchServiceImpl::process_spent_detail_full] Failed to bulk index")?;

            total_indexed += batch_count as u64;

            info!(
                "[BatchServiceImpl::process_spent_detail_full] Indexed {} documents so far",
                total_indexed
            );
        }

        // Step 4~5: 운영 설정 적용 + alias 스왑
        Self::finalize_full_index(elastic_service, index_alias, &new_index_name)
            .await
            .context("[BatchServiceImpl::process_spent_detail_full] finalize_full_index")?;

        info!(
            "[BatchServiceImpl::process_spent_detail_full] Completed successfully. Total indexed: {}",
            total_indexed
        );

        Ok(())
    }
    
    async fn process_spent_type_full(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let index_alias: &str = schedule_item.index_name();
        let batch_size: usize = *schedule_item.batch_size();

        info!(
            "[BatchServiceImpl::process_spent_type_full] Processing {} (index: {})",
            schedule_item.batch_name(),
            index_alias
        );

        // Step 1~2: 스키마 로드 + 벌크 인덱스 생성
        let new_index_name: String =
            Self::prepare_full_index(elastic_service, index_alias, schedule_item.mapping_schema())
                .await
                .context("[BatchServiceImpl::process_spent_type_full] prepare_full_index")?;

        // Step 3: MySQL에서 배치 단위로 조회하여 ES에 색인
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

        // Step 4~5: 운영 설정 적용 + alias 스왑
        Self::finalize_full_index(elastic_service, index_alias, &new_index_name)
            .await
            .context("[BatchServiceImpl::process_spent_type_full] finalize_full_index")?;

        info!(
            "[BatchServiceImpl::process_spent_type_full] Completed successfully. Total indexed: {}",
            total_indexed
        );

        Ok(())
    }
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
