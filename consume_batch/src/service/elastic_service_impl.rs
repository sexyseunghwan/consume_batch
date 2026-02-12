use crate::common::*;

use crate::service_trait::elastic_service::*;

use crate::repository::es_repository::*;

#[derive(Debug, Getters, Clone, new)]
pub struct ElasticServiceImpl<R: EsRepository> {
    elastic_conn: R,
}

#[async_trait]
impl<R> ElasticService for ElasticServiceImpl<R>
where
    R: EsRepository + Sync + Send,
{
    async fn create_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .create_index(index_name, settings, mappings)
            .await
    }

    async fn update_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .update_index_settings(index_name, settings)
            .await
    }

    async fn bulk_index<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> anyhow::Result<()> {
        self.elastic_conn.bulk_index(index_name, documents).await
    }

    async fn swap_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        self.elastic_conn
            .swap_alias(alias_name, new_index_name)
            .await
    }
}
