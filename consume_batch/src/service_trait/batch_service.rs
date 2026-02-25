use crate::common::*;
use crate::models::batch_schedule::BatchScheduleItem;

#[async_trait]
pub trait BatchService {
    async fn main_batch_task(&self) -> anyhow::Result<()>;
    async fn run_batch(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;
}
