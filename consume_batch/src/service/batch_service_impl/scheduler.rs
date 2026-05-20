//! Cron scheduler setup and immediate-job spawning.

use crate::models::batch_schedule::*;
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

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
    pub(super) async fn initialize_cron_scheduler(&self) -> anyhow::Result<JobScheduler> {
        let scheduler: JobScheduler = JobScheduler::new().await.inspect_err(|e| {
            error!(
                "[BatchServiceImpl::initialize_cron_scheduler] Failed to create JobScheduler: {:#}",
                e
            );
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

    pub(super) fn initialize_immediate_jobs(&self) -> JoinSet<()> {
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
            let smtp: Arc<S> = Arc::clone(&self.smtp_service);
            let item: BatchScheduleItem = immediate_item.clone();

            batch_log!(
                info,
                "[BatchServiceImpl::initialize_immediate_jobs] {} Spawning immediate job (one-time execution)",
                immediate_item.index_name()
            );

            immediate_jobs.spawn(async move {
                batch_log!(info, "[{}] Immediate job started", item.index_name());

                match Self::input_batch_by_schedule(
                    &item,
                    &mysql,
                    &elastic,
                    &public_data,
                    &indexing,
                    &smtp,
                )
                .await
                {
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

    pub(super) fn initialize_index_job(&self, schedule_item: &BatchScheduleItem) -> Result<Job> {
        let index_name: String = schedule_item.index_name().clone();
        let cron_expr: String = schedule_item.cron_schedule().clone();

        // Clone Arc references for the async closure
        // These clones are necessary because the closure must be 'static
        let mysql_service: Arc<M> = Arc::clone(&self.mysql_service);
        let elastic_service: Arc<E> = Arc::clone(&self.elastic_service);
        let public_data_service: Arc<D> = Arc::clone(&self.public_data_service);
        let indexing_service: Arc<I> = Arc::clone(&self.indexing_service);
        let smtp_service: Arc<S> = Arc::clone(&self.smtp_service);
        let schedule_item_move: BatchScheduleItem = schedule_item.clone();

        batch_log!(
            info,
            "[BatchServiceImpl::initialize_index_job] {} Registering cron job with schedule: {}",
            index_name,
            cron_expr
        );

        let job: Job = Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
            // Clone again for each job execution (closure is FnMut, called multiple times)
            let mysql: Arc<M> = Arc::clone(&mysql_service);
            let elastic: Arc<E> = Arc::clone(&elastic_service);
            let public_data: Arc<D> = Arc::clone(&public_data_service);
            let indexing: Arc<I> = Arc::clone(&indexing_service);
            let smtp: Arc<S> = Arc::clone(&smtp_service);
            let schedule_item: BatchScheduleItem = schedule_item_move.clone();

            Box::pin(async move {
                batch_log!(info, "[{}] Cron job triggered", schedule_item.index_name());

                match Self::input_batch_by_schedule(
                    &schedule_item,
                    &mysql,
                    &elastic,
                    &public_data,
                    &indexing,
                    &smtp,
                )
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
}
