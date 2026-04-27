use crate::common::*;
use crate::models::batch_schedule::BatchScheduleItem;

#[async_trait]
pub trait BatchService {
    /// Starts the batch scheduler and keeps the service alive until shutdown.
    async fn initialize_batch_task(&self) -> anyhow::Result<()>;
    /// Runs a single schedule item immediately.
    async fn input_batch(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;
}
