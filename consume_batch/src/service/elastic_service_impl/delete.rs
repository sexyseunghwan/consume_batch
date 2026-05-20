use crate::common::*;
use crate::repository::es_repository::EsRepository;

use super::ElasticServiceImpl;

impl<R: EsRepository + Sync + Send> ElasticServiceImpl<R> {
    pub(super) async fn delete_bulk(
        &self,
        index_name: &str,
        doc_ids: Vec<i64>,
    ) -> anyhow::Result<()> {
        self.elastic_conn.delete_bulk(index_name, doc_ids).await
    }

    pub(super) async fn delete_indices(&self, index_names: &[String]) -> anyhow::Result<()> {
        self.elastic_conn.delete_indices(index_names).await
    }
}
