use crate::common::*;

use crate::service_trait::elastic_service::*;

use crate::repository::es_repository::*;

use crate::app_config::AppConfig;

use crate::models::{ConsumingIndexProdtType, DocumentWithId, score_manager::*};

#[derive(Debug, Getters, Clone, new)]
pub struct ElasticServiceImpl<R: EsRepository> {
    elastic_conn: R,
}

#[async_trait]
impl<R> ElasticService for ElasticServiceImpl<R>
where
    R: EsRepository + Sync + Send,
{
    /// Creates a new Elasticsearch index using the repository layer.
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

    /// Updates settings for an existing Elasticsearch index.
    async fn update_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .update_index_settings(index_name, settings)
            .await
    }

    /// Bulk-indexes documents into the target Elasticsearch index.
    async fn bulk_index<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> anyhow::Result<()> {
        self.elastic_conn.bulk_index(index_name, documents).await
    }

    /// Bulk-updates documents in the target Elasticsearch index.
    async fn bulk_update<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: &str,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .bulk_update(index_name, documents, doc_id_field)
            .await
    }

    /// Bulk-deletes documents from the target Elasticsearch index.
    async fn bulk_delete(&self, index_name: &str, doc_ids: Vec<i64>) -> anyhow::Result<()> {
        self.elastic_conn.bulk_delete(index_name, doc_ids).await
    }

    /// Repoints an alias to a new Elasticsearch index.
    async fn swap_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        self.elastic_conn
            .swap_alias(alias_name, new_index_name)
            .await
    }

    /// Updates the write alias so new writes target `target_index`.
    async fn update_write_alias(
        &self,
        write_alias: &str,
        target_index: &str,
    ) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::update_write_alias] Updating write alias '{}' to point to '{}'",
            write_alias, target_index
        );

        // Write alias should point to exactly one index (no traffic splitting)
        self.elastic_conn
            .swap_alias(write_alias, target_index)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::update_write_alias] Failed to update write alias '{}' to '{}': {:#}", write_alias, target_index, e);
            })
    }

    /// Updates the read alias so queries resolve to `target_index`.
    async fn update_read_alias(&self, read_alias: &str, target_index: &str) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::update_read_alias] Updating read alias '{}' to point to '{}'",
            read_alias, target_index
        );

        // Write alias should point to exactly one index (no traffic splitting)
        self.elastic_conn
            .swap_alias(read_alias, target_index)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::update_read_alias] Failed to update read alias '{}' to '{}': {:#}", read_alias, target_index, e);
            })
    }

    /// Converts an Elasticsearch search response into typed documents with IDs and scores.
    async fn get_query_result_vec<T: DeserializeOwned>(
        &self,
        response_body: &Value,
    ) -> Result<Vec<DocumentWithId<T>>, anyhow::Error> {
        let hits: &Value = &response_body["hits"]["hits"];

        let results: Vec<DocumentWithId<T>> = hits
            .as_array()
            .ok_or_else(|| anyhow!("[Error][get_query_result_vec()] 'hits' field is not an array"))?
            .iter()
            .map(|hit| {
                let id: &str = hit.get("_id").and_then(|id| id.as_str()).ok_or_else(|| {
                    anyhow!("[Error][get_query_result_vec()] Missing '_id' field")
                })?;

                let source: &Value = hit.get("_source").ok_or_else(|| {
                    anyhow!("[Error][get_query_result_vec()] Missing '_source' field")
                })?;

                let source: T = serde_json::from_value(source.clone()).map_err(|e| {
                    anyhow!(
                        "[Error][get_query_result_vec()] Failed to deserialize source: {:?}",
                        e
                    )
                })?;

                let score: f64 = hit
                    .get("_score")
                    .and_then(|score| score.as_f64())
                    .unwrap_or(0.0);

                Ok::<DocumentWithId<T>, anyhow::Error>(DocumentWithId {
                    id: id.to_string(),
                    score,
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Restores production-oriented settings on a completed index and refreshes it.
    async fn revert_index_setting(&self, index_name: &str) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::revert_index_setting] Updating index settings for production: {}",
            index_name
        );

        let production_settings: Value = json!({
            "index": {
                "number_of_replicas": 2,
                "refresh_interval": "1s"
            }
        });

        self.update_index_settings(index_name, &production_settings)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::revert_index_setting] Failed to update index settings: {:#}", e);
            })?;

        self.elastic_conn
            .refresh_index(index_name)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::revert_index_setting] Failed to refresh index: {:#}",
                    e
                );
            })?;

        Ok(())
    }

    /// Prepares a new timestamped index for a full indexing run.
    async fn prepare_full_index(
        &self,
        index_name: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String> {
        let schema_content: String = tokio::fs::read_to_string(mapping_schema_path)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::prepare_full_index] Failed to read mapping schema file: {}: {:#}", mapping_schema_path, e);
            })?;

        let schema: Value = serde_json::from_str(&schema_content).inspect_err(|e| {
            error!(
                "[ElasticServiceImpl::prepare_full_index] Failed to parse mapping schema: {:#}",
                e
            );
        })?;

        let mappings: Value = schema
            .get("mappings")
            .ok_or_else(|| {
                anyhow!("[ElasticServiceImpl::prepare_full_index] Missing 'mappings' in schema")
            })?
            .clone();

        let mut settings: Value = schema.get("settings").cloned().unwrap_or_else(|| json!({}));

        // Merge bulk indexing settings for fast initial load
        if let Some(index_obj) = settings.get_mut("index") {
            if let Some(obj) = index_obj.as_object_mut() {
                obj.insert("number_of_shards".to_string(), json!(3));
                obj.insert("number_of_replicas".to_string(), json!(0));
                obj.insert("refresh_interval".to_string(), json!("-1"));
            }
        } else {
            settings["index"]["number_of_shards"] = json!(3);
            settings["index"]["number_of_replicas"] = json!(0);
            settings["index"]["refresh_interval"] = json!("-1");
        }

        let new_index_name: String =
            format!("{}_{}", index_name, Utc::now().format("%Y%m%d%H%M%S"));

        info!(
            "[ElasticServiceImpl::prepare_full_index] Creating new index: {}",
            new_index_name
        );

        self.create_index(&new_index_name, &settings, &mappings)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::prepare_full_index] Failed to create index: {:#}",
                    e
                );
            })?;

        Ok(new_index_name)
    }

    /// Deletes Elasticsearch indices that are no longer needed.
    async fn delete_indices(&self, index_names: &[String]) -> anyhow::Result<()> {
        self.elastic_conn.delete_indices(index_names).await
    }

    /// Resolves an alias to the list of Elasticsearch indices behind it.
    async fn get_index_name_by_alias(&self, alias: &str) -> anyhow::Result<Vec<String>> {
        self.elastic_conn
            .get_index_by_alias(alias)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::get_index_name_by_alias] Failed to resolve alias '{}': {:#}",
                    alias, e
                );
            })
    }

    /// Predicts the consume keyword type for a given product name.
    async fn get_consume_type_judgement(
        &self,
        prodt_name: &str,
    ) -> Result<ConsumingIndexProdtType, anyhow::Error> {
        let app_config: &AppConfig = AppConfig::global();
        let es_spent_type: &str = app_config.es_spent_type().as_str();

        let es_query: Value = json!({
            "query": {
                "match": {
                    "consume_keyword": prodt_name
                }
            }
        });

        let response_body: Value = self
            .elastic_conn
            .get_search_query(&es_query, es_spent_type)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::get_consume_type_judgement] response_body: {:?}",
                    e
                )
            })?;

        let results: Vec<DocumentWithId<ConsumingIndexProdtType>> = self
            .get_query_result_vec(&response_body)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::get_consume_type_judgement] results: {:?}",
                    e
                )
            })?;

        if results.is_empty() {
            return Ok(ConsumingIndexProdtType::new(
                20,
                String::from("etc"),
                prodt_name.to_string(),
                0,
            ));
        } else {
            let mut manager: ScoreManager<ConsumingIndexProdtType> =
                ScoreManager::<ConsumingIndexProdtType>::new();

            for consume_type in results {
                let keyword_weight: f64 = *consume_type.source().keyword_weight() as f64;
                let score: f64 = *consume_type.score() * -1.0 * keyword_weight;
                let score_i64: i64 = score as i64;
                let keyword: &str = consume_type.source.consume_keyword();

                /* Use the 'levenshtein' algorithm to determine word match */
                let word_dist: usize = levenshtein(keyword, prodt_name);
                let word_dist_i64: i64 = word_dist.try_into()?;
                manager.insert(word_dist_i64 + score_i64, consume_type.source);
            }

            let score_data_keyword: ScoredData<ConsumingIndexProdtType> = match manager.pop_lowest()
            {
                Some(score_data_keyword) => score_data_keyword,
                None => {
                    return Err(anyhow!(
                        "[ElasticServiceImpl::get_consume_type_judgement] The mapped data for variable 'score_data_keyword' does not exist."
                    ));
                }
            };

            return Ok(score_data_keyword.data);
        }
    }
}
