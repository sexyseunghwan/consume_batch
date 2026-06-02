//! Indexing service implementation.
//!
//! # Submodule Layout
//!
//! | File              | Responsibility                                         |
//! |-------------------|--------------------------------------------------------|
//! | `mod.rs`          | Struct, `new()`, `IndexingService` trait impl,         |
//! |                   | `delete_orphaned_index` shared cleanup helper          |
//! | `spent_detail.rs` | SpentDetail full/incremental/catch-up pipelines        |
//! | `spent_type.rs`   | SpentType full indexing pipeline                       |

mod spent_detail;
mod spent_type;

use crate::common::*;
use crate::models::batch_schedule::BatchScheduleItem;
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService,
};

#[derive(Debug, Getters)]
#[getset(get = "pub")]
pub struct IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService,
    P: ProducerService,
    E: ElasticService,
    C: ConsumeService,
{
    pub(super) mysql_service: Arc<M>,
    #[allow(dead_code)]
    pub(super) producer_service: Arc<P>,
    pub(super) elastic_service: Arc<E>,
    pub(super) consume_service: Arc<C>,
}

impl<M, P, E, C> IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    pub fn new(
        mysql_service: Arc<M>,
        producer_service: Arc<P>,
        elastic_service: Arc<E>,
        consume_service: Arc<C>,
    ) -> Self {
        Self {
            mysql_service,
            producer_service,
            elastic_service,
            consume_service,
        }
    }

    pub(super) async fn delete_orphaned_index(&self, index_name: &str) {
        error!(
            "[IndexingServiceImpl::delete_orphaned_index] Cleaning up orphaned index '{}' due to pipeline failure",
            index_name
        );
        if let Err(e) = self
            .elastic_service
            .delete_indices(&[index_name.to_string()])
            .await
        {
            error!(
                "[IndexingServiceImpl::delete_orphaned_index] Failed to delete orphaned index '{}': {:#}",
                index_name, e
            );
        } else {
            error!(
                "[IndexingServiceImpl::delete_orphaned_index] Successfully deleted orphaned index '{}'",
                index_name
            );
        }
    }
}

#[async_trait]
impl<M, P, E, C> IndexingService for IndexingServiceImpl<M, P, E, C>
where
    M: MysqlService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
{
    async fn input_spent_detail_full(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        self.input_spent_detail_full(schedule_item).await
    }

    async fn input_spent_detail_incremental(
        &self,
        schedule_item: &BatchScheduleItem,
    ) -> anyhow::Result<()> {
        self.input_spent_detail_incremental(schedule_item).await
    }

    async fn input_spent_type_full(&self, schedule_item: &BatchScheduleItem) -> anyhow::Result<()> {
        self.input_spent_type_full(schedule_item).await
    }
}
