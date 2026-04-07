use crate::common::*;
use crate::models::batch_schedule::BatchScheduleItem;

#[async_trait]
pub trait IndexingService: Send + Sync {
    /// MySQL → Kafka 전체 마이그레이션
    // async fn run_spent_detail_migration_to_kafka(
    //     &self,
    //     schedule_item: &BatchScheduleItem,
    // ) -> anyhow::Result<()>;

    /// 전체 색인 (migration → static → dynamic → alias swap)
    async fn run_spent_detail_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;

    //async fn run_spent_detail_full_v1(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;    

    /// 증분 색인 (무한 루프, write alias 기준)
    async fn run_spent_detail_incremental(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()>;

    /// SpentType 키워드 전체 색인 (MySQL → ES, alias swap)
    async fn run_spent_type_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()>;
}
