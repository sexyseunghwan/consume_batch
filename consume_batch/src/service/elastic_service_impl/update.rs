use crate::common::*;
use crate::repository::es_repository::EsRepository;

use super::ElasticServiceImpl;

impl<R: EsRepository + Sync + Send> ElasticServiceImpl<R> {
    pub(super) async fn modify_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_index_settings(index_name, settings)
            .await
    }

    pub(super) async fn modify_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: &str,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_bulk(index_name, documents, doc_id_field)
            .await
    }

    pub(super) async fn modify_alias(
        &self,
        alias_name: &str,
        new_index_name: &str,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_alias(alias_name, new_index_name)
            .await
    }

    pub(super) async fn modify_write_alias(
        &self,
        write_alias: &str,
        target_index: &str,
    ) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::modify_write_alias] Updating write alias '{}' to point to '{}'",
            write_alias, target_index
        );

        self.elastic_conn
            .modify_alias(write_alias, target_index)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::modify_write_alias] Failed to update write alias '{}' to '{}': {:#}",
                    write_alias, target_index, e
                );
            })
    }

    pub(super) async fn modify_read_alias(
        &self,
        read_alias: &str,
        target_index: &str,
    ) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::modify_read_alias] Updating read alias '{}' to point to '{}'",
            read_alias, target_index
        );

        self.elastic_conn
            .modify_alias(read_alias, target_index)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::modify_read_alias] Failed to update read alias '{}' to '{}': {:#}",
                    read_alias, target_index, e
                );
            })
    }

    pub(super) async fn modify_index_setting(&self, index_name: &str) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::modify_index_setting] Updating index settings for production: {}",
            index_name
        );

        let production_settings: Value = json!({
            "index": {
                "number_of_replicas": 2,
                "refresh_interval": "1s"
            }
        });

        self.modify_index_settings(index_name, &production_settings)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::modify_index_setting] Failed to update index settings: {:#}",
                    e
                );
            })?;

        self.elastic_conn
            .modify_index_refresh(index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::modify_index_setting] Failed to refresh index: {:#}",
                    e
                );
            })?;

        Ok(())
    }
}
