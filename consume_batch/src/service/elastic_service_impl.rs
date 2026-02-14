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

    async fn bulk_update<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> anyhow::Result<()> {
        self.elastic_conn.bulk_update(index_name, documents).await
    }

    async fn bulk_delete(
        &self,
        index_name: &str,
        doc_ids: Vec<i64>,
    ) -> anyhow::Result<()> {
        self.elastic_conn.bulk_delete(index_name, doc_ids).await
    }

    async fn swap_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        self.elastic_conn
            .swap_alias(alias_name, new_index_name)
            .await
    }

    async fn update_write_alias(&self, write_alias: &str, target_index: &str) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::update_write_alias] Updating write alias '{}' to point to '{}'",
            write_alias, target_index
        );

        // Write alias should point to exactly one index (no traffic splitting)
        self.elastic_conn
            .swap_alias(write_alias, target_index)
            .await
            .context(format!(
                "[ElasticServiceImpl::update_write_alias] Failed to update write alias '{}' to '{}'",
                write_alias, target_index
            ))
    }

    async fn update_read_alias_with_weight(
        &self,
        read_alias: &str,
        new_index: &str,
        traffic_weight: f32,
    ) -> anyhow::Result<()> {
        info!(
            "[ElasticServiceImpl::update_read_alias_with_weight] Updating read alias '{}' with {}% traffic to '{}'",
            read_alias, traffic_weight * 100.0, new_index
        );

        // Validate traffic_weight range
        if !(0.0..=1.0).contains(&traffic_weight) {
            return Err(anyhow!(
                "[ElasticServiceImpl::update_read_alias_with_weight] Invalid traffic_weight: {}. Must be between 0.0 and 1.0",
                traffic_weight
            ));
        }

        // If traffic_weight is 1.0, simply swap the alias completely
        if traffic_weight >= 0.99 {
            info!("[ElasticServiceImpl::update_read_alias_with_weight] Full traffic (100%), performing complete swap");
            return self.swap_alias(read_alias, new_index).await;
        }

        // If traffic_weight is 0.0, do nothing (keep old index)
        if traffic_weight <= 0.01 {
            info!("[ElasticServiceImpl::update_read_alias_with_weight] Zero traffic (0%), skipping update");
            return Ok(());
        }

        // For partial traffic (0 < weight < 1), use routing-based traffic splitting
        // This requires Elasticsearch routing support
        warn!(
            "[ElasticServiceImpl::update_read_alias_with_weight] Partial traffic split ({:.1}%) is not fully implemented yet. Defaulting to full swap.",
            traffic_weight * 100.0
        );

        // TODO: Implement actual weighted routing via:
        // - Index-level routing configuration
        // - Application-level client-side routing based on weight
        // For now, fallback to complete swap when weight > 0.5
        if traffic_weight > 0.5 {
            self.swap_alias(read_alias, new_index).await
        } else {
            Ok(())
        }
    }

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

        if results.len() == 0 {
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
                let word_dist: usize = levenshtein(keyword, &prodt_name);
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
