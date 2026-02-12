use crate::common::*;

#[async_trait]
pub trait BatchService {
    async fn main_batch_task(&self) -> anyhow::Result<()>;
}
