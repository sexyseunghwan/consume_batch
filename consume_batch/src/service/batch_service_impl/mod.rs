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
//! # Submodule Layout
//!
//! | File              | Responsibility                                      |
//! |-------------------|-----------------------------------------------------|
//! | `mod.rs`          | Struct definition, lifecycle, `BatchService` impl   |
//! | `scheduler.rs`    | Cron scheduler setup and immediate-job spawning     |
//! | `dispatcher.rs`   | Route `batch_name` → concrete handler               |
//! | `type_update.rs`  | Bulk re-classify `consume_keyword_type` columns     |
//! | `report.rs`       | Monthly spend report via SMTP                       |
//! | `dimension.rs`    | Populate `DIM_CALENDAR` date dimension table        |

mod asset;
mod dimension;
mod dispatcher;
mod report;
mod scheduler;
mod type_update;

use crate::models::batch_schedule::*;
use crate::service_trait::{
    batch_service::*, consume_service::ConsumeService, elastic_service::*, indexing_service::*,
    mysql_service::*, producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService, redis_service::*
};
use crate::{app_config::*, batch_log, common::*};

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
/// * `S` - SMTP email service implementation
///
/// # Thread Safety
///
/// The service is wrapped in `Arc` internally to allow safe sharing
/// across multiple async tasks spawned by the scheduler.
#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService,
    E: ElasticService,
    C: ConsumeService,
    P: ProducerService,
    D: PublicDataService,
    I: IndexingService,
    S: SmtpService,
    R: RedisService
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
    /// Service for sending SMTP emails.
    smtp_service: Arc<S>,
    redis_service: Arc<R>,
}

// Manual Clone impl: Arc<T> is always Clone regardless of T: Clone
impl<M, E, C, P, D, I, S, R> Clone for BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService,
    E: ElasticService,
    C: ConsumeService,
    P: ProducerService,
    D: PublicDataService,
    I: IndexingService,
    S: SmtpService,
    R: RedisService
{
    fn clone(&self) -> Self {
        Self {
            mysql_service: Arc::clone(&self.mysql_service),
            elastic_service: Arc::clone(&self.elastic_service),
            consume_service: Arc::clone(&self.consume_service),
            schedule_config: self.schedule_config.clone(),
            producer_service: Arc::clone(&self.producer_service),
            public_data_service: Arc::clone(&self.public_data_service),
            indexing_service: Arc::clone(&self.indexing_service),
            smtp_service: Arc::clone(&self.smtp_service),
            redis_service: Arc::clone(&self.redis_service)
        }
    }
}

impl<M, E, C, P, D, I, S, R> BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
    R: RedisService + Send + Sync + 'static
{
    pub fn new(
        mysql_service: Arc<M>,
        elastic_service: Arc<E>,
        consume_service: Arc<C>,
        producer_service: Arc<P>,
        public_data_service: D,
        indexing_service: I,
        smtp_service: S,
        redis_service: R
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
            smtp_service: Arc::new(smtp_service),
            redis_service: Arc::new(redis_service)
        })
    }

    pub fn find_enabled_schedules(&self) -> Vec<&BatchScheduleItem> {
        self.schedule_config.find_enabled_schedules()
    }
}

#[async_trait]
impl<M, E, C, P, D, I, S, R> BatchService for BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
    R: RedisService + Send + Sync + 'static
{
    async fn initialize_batch_task(&self) -> anyhow::Result<()> {
        batch_log!(
            info,
            "[BatchServiceImpl::initialize_batch_task] Starting batch service main task"
        );

        let mut immediate_jobs: JoinSet<()> = self.initialize_immediate_jobs();     // 애는 즉시 한번 실행되어야 함
        let mut scheduler: JobScheduler = self.initialize_cron_scheduler().await?;  // 동시에 스케쥴러는 계속 실행되어야 함.
        
        batch_log!(
            info,
            "[BatchServiceImpl::initialize_batch_task] Scheduler is running. Press Ctrl+C to shutdown gracefully."
        );

        /*
            tokio::select! 는 Rust async code 에서 여러 비동기 작업을 동시에 기다리다가, 
            먼저 완료되는 작업 하나를 처리하는 메크로임.
        */
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                batch_log!(info,
                    "[BatchServiceImpl::initialize_batch_task] Shutdown signal received, stopping scheduler..."
                );
            }
            _ = async {
                while let Some(result) = immediate_jobs.join_next().await {
                    if let Err(e) = result {
                        batch_log!(error, "[BatchServiceImpl::initialize_batch_task] Immediate job panicked: {:?}", e);
                    }
                }
                batch_log!(info, "[BatchServiceImpl::initialize_batch_task] All immediate jobs completed, keeping service alive until Ctrl+C...");
                std::future::pending::<()>().await; 
            } => {}
        }

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

    async fn input_batch(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        Self::input_batch_by_schedule(
            schedule_item,
            &self.mysql_service,
            &self.elastic_service,
            &self.public_data_service,
            &self.indexing_service,
            &self.smtp_service,
        )
        .await
    }
}
