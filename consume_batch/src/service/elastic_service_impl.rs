use crate::common::*;

use crate::service_trait::elastic_service::*;

use crate::repository::es_repository::*;

use crate::app_config::AppConfig;

use crate::models::{ConsumingIndexProdtType, DocumentWithId, score_manager::*};

#[derive(Debug, Getters, Clone, new)]
pub struct ElasticServiceImpl<R: EsRepository> {
    elastic_conn: R,
}

impl<R: EsRepository> ElasticServiceImpl<R> {
    fn find_consume_type(
        prodt_name: &str,
        results: Vec<DocumentWithId<ConsumingIndexProdtType>>,
    ) -> anyhow::Result<ConsumingIndexProdtType> {
        if results.is_empty() {
            return Ok(ConsumingIndexProdtType::new(
                20,
                String::from("etc"),
                prodt_name.to_string(),
                0,
            ));
        }

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
            manager.input(word_dist_i64 + score_i64, consume_type.source);
        }

        let score_data_keyword: ScoredData<ConsumingIndexProdtType> =
            manager.find_lowest().ok_or_else(|| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type] The mapped data for variable 'score_data_keyword' does not exist."
                )
            })?;

        Ok(score_data_keyword.data)
    }
}

#[async_trait]
impl<R> ElasticService for ElasticServiceImpl<R>
where
    R: EsRepository + Sync + Send,
{
    /// Creates a new Elasticsearch index using the repository layer.
    async fn initialize_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .initialize_index(index_name, settings, mappings)
            .await
    }

    /// Updates settings for an existing Elasticsearch index.
    async fn modify_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_index_settings(index_name, settings)
            .await
    }

    /// Bulk-indexes documents into the target Elasticsearch index.
    async fn input_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: Option<&str>,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .input_bulk(index_name, documents, doc_id_field)
            .await
    }

    /// Bulk-updates documents in the target Elasticsearch index.
    async fn modify_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: &str,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_bulk(index_name, documents, doc_id_field)
            .await
    }

    /// Bulk-deletes documents from the target Elasticsearch index.
    async fn delete_bulk(&self, index_name: &str, doc_ids: Vec<i64>) -> anyhow::Result<()> {
        self.elastic_conn.delete_bulk(index_name, doc_ids).await
    }

    /// Repoints an alias to a new Elasticsearch index.
    async fn modify_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        self.elastic_conn
            .modify_alias(alias_name, new_index_name)
            .await
    }

    /// Updates the write alias so new writes target `target_index`.
    async fn modify_write_alias(
        &self,
        write_alias: &str,
        target_index: &str,
    ) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::modify_write_alias] Updating write alias '{}' to point to '{}'",
            write_alias, target_index
        );

        // Write alias should point to exactly one index (no traffic splitting)
        self.elastic_conn
            .modify_alias(write_alias, target_index)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::modify_write_alias] Failed to update write alias '{}' to '{}': {:#}", write_alias, target_index, e);
            })
    }

    /// Updates the read alias so queries resolve to `target_index`.
    async fn modify_read_alias(&self, read_alias: &str, target_index: &str) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::modify_read_alias] Updating read alias '{}' to point to '{}'",
            read_alias, target_index
        );

        // Write alias should point to exactly one index (no traffic splitting)
        self.elastic_conn
            .modify_alias(read_alias, target_index)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::modify_read_alias] Failed to update read alias '{}' to '{}': {:#}", read_alias, target_index, e);
            })
    }

    /// Converts an Elasticsearch search response into typed documents with IDs and scores.
    async fn find_query_result_vec<T: DeserializeOwned>(
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
    async fn modify_index_setting(&self, index_name: &str) -> anyhow::Result<()> {
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
                error!("[ElasticServiceImpl::modify_index_setting] Failed to update index settings: {:#}", e);
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

    /// Prepares a new timestamped index for a full indexing run.
    async fn initialize_full_index(
        &self,
        index_name: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String> {
        let schema_content: String = tokio::fs::read_to_string(mapping_schema_path)
            .await
            .inspect_err(|e| {
                error!("[ElasticServiceImpl::initialize_full_index] Failed to read mapping schema file: {}: {:#}", mapping_schema_path, e);
            })?;

        let schema: Value = serde_json::from_str(&schema_content).inspect_err(|e| {
            error!(
                "[ElasticServiceImpl::initialize_full_index] Failed to parse mapping schema: {:#}",
                e
            );
        })?;

        let mappings: Value = schema
            .get("mappings")
            .ok_or_else(|| {
                anyhow!("[ElasticServiceImpl::initialize_full_index] Missing 'mappings' in schema")
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
            "[ElasticServiceImpl::initialize_full_index] Creating new index: {}",
            new_index_name
        );

        self.initialize_index(&new_index_name, &settings, &mappings)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::initialize_full_index] Failed to create index: {:#}",
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
    async fn find_index_name_by_alias(&self, alias: &str) -> anyhow::Result<Vec<String>> {
        self.elastic_conn
            .find_index_by_alias(alias)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::find_index_name_by_alias] Failed to resolve alias '{}': {:#}",
                    alias, e
                );
            })
    }

    /// Predicts the consume keyword type for a given product name.
    async fn find_consume_type_judgement(
        &self,
        prodt_name: &str,
    ) -> Result<ConsumingIndexProdtType, anyhow::Error> {
        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!("[ElasticServiceImpl::find_consume_type_judgement] app_config: {:#}", e);
        })?;
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
            .find_by_query(&es_query, es_spent_type)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type_judgement] response_body: {:?}",
                    e
                )
            })?;

        let results: Vec<DocumentWithId<ConsumingIndexProdtType>> = self
            .find_query_result_vec(&response_body)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type_judgement] results: {:?}",
                    e
                )
            })?;

        Self::find_consume_type(prodt_name, results)
    }

    /// Predicts the consume keyword types for multiple product names using a single msearch call.
    async fn find_consume_type_judgements(
        &self,
        prodt_names: &[String],
    ) -> Result<Vec<ConsumingIndexProdtType>, anyhow::Error> {
        
        if prodt_names.is_empty() {
            return Ok(Vec::new());
        }

        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!("[ElasticServiceImpl::find_consume_type_judgements] app_config: {:#}", e);
        })?;
        let es_spent_type_index: &str = app_config.es_spent_type().as_str(); // Name of Elasticsearch index.
        
        let es_queries: Vec<Value> = prodt_names
            .iter()
            .map(|prodt_name| {
                json!({
                    "query": {
                        "match": {
                            "consume_keyword": prodt_name
                        }
                    }
                })
            })
            .collect();

        let response_bodies: Vec<Value> = self
            .elastic_conn
            .finds_by_query(&es_queries, es_spent_type_index)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type_judgements] response_body: {:?}",
                    e
                )
            })?;
        
        let mut consume_types: Vec<ConsumingIndexProdtType> =
            Vec::with_capacity(response_bodies.len());
        
        /* 
            prodt_names = ["네이버페이", "쿠팡", "카카오"]
            response_bodies = [
                {
                    "status": 200,
                    "took": 12,
                    "timed_out": false,
                    "_shards": {
                        "total": 3,
                        "successful": 3,
                        "skipped": 0,
                        "failed": 0
                    },
                    "hits": {
                        "total": {
                        "value": 6,
                        "relation": "eq"
                        },
                        "max_score": 7.8795195,
                        "hits": [
                        {
                            "_id": "Avk4mp0BII1OZBPIKoES",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 7.8795195,
                            "_source": {
                            "consume_keyword": "네이버페이",
                            "consume_keyword_type": "인터넷 쇼핑",
                            "consume_keyword_type_id": 16,
                            "keyword_weight": 1
                            }
                        },
                        {
                            "_id": "Afk4mp0BII1OZBPIKoES",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 4.8827868,
                            "_source": {
                            "consume_keyword": "네이버",
                            "consume_keyword_type": "인터넷 쇼핑",
                            "consume_keyword_type_id": 16,
                            "keyword_weight": 1
                            }
                        },
                        {
                            "_id": "D_k4mp0BII1OZBPIKoET",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 3.9397597,
                            "_source": {
                            "consume_keyword": "카카오페이",
                            "consume_keyword_type": "인터넷 쇼핑",
                            "consume_keyword_type_id": 16,
                            "keyword_weight": 1
                            }
                        },
                        {
                            "_id": "gPk4mp0BII1OZBPIKoAS",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 3.7906468,
                            "_source": {
                            "consume_keyword": "비플페이",
                            "consume_keyword_type": "식사",
                            "consume_keyword_type_id": 13,
                            "keyword_weight": 1
                            }
                        },
                        {
                            "_id": "Bfk4mp0BII1OZBPIKoET",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 3.7906468,
                            "_source": {
                            "consume_keyword": "삼성페이",
                            "consume_keyword_type": "인터넷 쇼핑",
                            "consume_keyword_type_id": 16,
                            "keyword_weight": 1
                            }
                        },
                        {
                            "_id": "Efk4mp0BII1OZBPIKoET",
                            "_index": "spent_type_dev_20260417065424",
                            "_score": 3.7906468,
                            "_source": {
                            "consume_keyword": "쿠팡 페이",
                            "consume_keyword_type": "인터넷 쇼핑",
                            "consume_keyword_type_id": 16,
                            "keyword_weight": 1
                            }
                        }
                        ]
                    }
                },  => A1: Reesult of "네이버페이" 
                ... => A2: Reesult of "쿠팡" 
                ... => A3: Reesult of "카카오"
            ]
            
            "네이버페이", "쿠팡", "카카오"

            prodt_names.iter().zip(response_bodies.iter())
            => 
            [
                ("네이버페이", A1),
                ("쿠팡", A2),
                ("카카오", A3)
            ]
        */
        for (prodt_name, response_body) in prodt_names.iter().zip(response_bodies.iter()) {
            let results: Vec<DocumentWithId<ConsumingIndexProdtType>> = self
                .find_query_result_vec(response_body)
                .await
                .map_err(|e| {
                    anyhow!(
                        "[ElasticServiceImpl::find_consume_type_judgements] results: {:?}",
                        e
                    )
                })?;

            consume_types.push(Self::find_consume_type(prodt_name, results)?);
        }

        /*
            consume_types = [
                {
                    "consume_keyword": "네이버페이",
                    "consume_keyword_type": "인터넷 쇼핑",
                    "consume_keyword_type_id": 16,
                    "keyword_weight": 1
                },
                {
                    "consume_keyword": "쿠팡",
                    "consume_keyword_type": "인터넷 쇼핑",
                    "consume_keyword_type_id": 16,
                    "keyword_weight": 1
                },
                {
                    "consume_keyword": "카카오",
                    "consume_keyword_type": "인터넷 쇼핑",
                    "consume_keyword_type_id": 16,
                    "keyword_weight": 1
                }
            ]
        */
        Ok(consume_types)
    }
}
