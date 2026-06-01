//! Elasticsearch service implementation.
//!
//! # Submodule Layout
//!
//! | File        | Responsibility                                  |
//! |-------------|-------------------------------------------------|
//! | `mod.rs`    | Struct definition, `ElasticService` trait impl  |
//! | `select.rs` | All `find_*` query methods                      |
//! | `insert.rs` | Index initialization and bulk index methods     |
//! | `update.rs` | All `modify_*` update methods                   |
//! | `delete.rs` | All `delete_*` methods                          |

mod delete;
mod insert;
mod select;
mod update;

use crate::common::*;
use crate::dtos::GroupSeqAggsRangeQuery;
use crate::models::{AggResultSet, ConsumingIndexProdtType, DocumentWithId};
use crate::repository::es_repository::EsRepository;
use crate::service_trait::elastic_service::ElasticService;

#[derive(Debug, Getters, Clone, new)]
pub struct ElasticServiceImpl<R: EsRepository> {
    pub(super) elastic_conn: R,
}

#[async_trait]
impl<R> ElasticService for ElasticServiceImpl<R>
where
    R: EsRepository + Sync + Send,
{
    async fn initialize_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()> {
        self.initialize_index(index_name, settings, mappings).await
    }

    async fn modify_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        self.modify_index_settings(index_name, settings).await
    }

    async fn input_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: Option<&str>,
    ) -> anyhow::Result<()> {
        self.input_bulk(index_name, documents, doc_id_field).await
    }

    async fn modify_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: &str,
    ) -> anyhow::Result<()> {
        self.modify_bulk(index_name, documents, doc_id_field).await
    }

    async fn delete_bulk(&self, index_name: &str, doc_ids: Vec<i64>) -> anyhow::Result<()> {
        self.delete_bulk(index_name, doc_ids).await
    }

    async fn modify_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        self.modify_alias(alias_name, new_index_name).await
    }

    async fn modify_write_alias(
        &self,
        write_alias: &str,
        target_index: &str,
    ) -> anyhow::Result<()> {
        self.modify_write_alias(write_alias, target_index).await
    }

    async fn modify_read_alias(&self, read_alias: &str, target_index: &str) -> anyhow::Result<()> {
        self.modify_read_alias(read_alias, target_index).await
    }

    async fn find_query_result_vec<T: DeserializeOwned>(
        &self,
        response_body: &Value,
    ) -> Result<Vec<DocumentWithId<T>>, anyhow::Error> {
        self.find_query_result_vec(response_body).await
    }

    async fn finalize_index_settings(&self, index_name: &str) -> anyhow::Result<()> {
        self.finalize_index_settings(index_name).await
    }

    async fn initialize_full_index(
        &self,
        index_name: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String> {
        self.initialize_full_index(index_name, mapping_schema_path)
            .await
    }

    async fn delete_indices(&self, index_names: &[String]) -> anyhow::Result<()> {
        self.delete_indices(index_names).await
    }

    async fn find_index_name_by_alias(&self, alias: &str) -> anyhow::Result<Vec<String>> {
        self.find_index_name_by_alias(alias).await
    }

    async fn find_consume_type_judgement(
        &self,
        prodt_name: &str,
    ) -> Result<ConsumingIndexProdtType, anyhow::Error> {
        self.find_consume_type_judgement(prodt_name).await
    }

    async fn find_consume_type_judgements(
        &self,
        prodt_names: &[String],
    ) -> Result<Vec<ConsumingIndexProdtType>, anyhow::Error> {
        self.find_consume_type_judgements(prodt_names).await
    }

    async fn find_info_filter_groupseq_orderby_aggs_range<T: Send + Sync + DeserializeOwned>(
        &self,
        query_options: GroupSeqAggsRangeQuery<'_>,
    ) -> Result<AggResultSet<T>, anyhow::Error> {
        self.find_info_filter_groupseq_orderby_aggs_range(query_options)
            .await
    }
}
