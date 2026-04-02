use crate::common::*;
use crate::models::batch_schedule::BatchScheduleItem;

#[async_trait]
pub trait BatchService {
    /// Starts the batch scheduler and keeps the service alive until shutdown.
    async fn main_batch_task(&self) -> anyhow::Result<()>;
    /// Runs a single schedule item immediately.
    async fn run_batch(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;
}
