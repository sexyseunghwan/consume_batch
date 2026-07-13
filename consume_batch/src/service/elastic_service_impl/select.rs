use crate::app_config::AppConfig;
use crate::common::*;
use crate::dtos::{
    AggResultSet, ConsumeKeywordTypeResult, DocumentWithId, GroupAggregationRangeQuery,
};
use crate::models::score_manager::*;
use crate::repository::es_repository::EsRepository;

use super::ElasticServiceImpl;

impl<R: EsRepository + Sync + Send> ElasticServiceImpl<R> {
    fn find_consume_type(
        product_name: &str,
        results: Vec<DocumentWithId<ConsumeKeywordTypeResult>>,
    ) -> anyhow::Result<ConsumeKeywordTypeResult> {
        if results.is_empty() {
            return Ok(ConsumeKeywordTypeResult::new(
                20,
                String::from("etc"),
                product_name.to_string(),
                0,
            ));
        }

        let mut manager: ScoreManager<ConsumeKeywordTypeResult> =
            ScoreManager::<ConsumeKeywordTypeResult>::new();

        for consume_type in results {
            let keyword_weight: f64 = *consume_type.source().keyword_weight() as f64;
            let score: f64 = *consume_type.score() * -1.0 * keyword_weight;
            let score_i64: i64 = score as i64;
            let keyword: &str = consume_type.source.consume_keyword();

            let word_dist: usize = levenshtein(keyword, product_name);
            let word_dist_i64: i64 = word_dist.try_into()?;
            manager.insert(word_dist_i64 + score_i64, consume_type.source);
        }

        let score_data_keyword: ScoredData<ConsumeKeywordTypeResult> =
            manager.find_lowest().ok_or_else(|| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type] The mapped data for variable 'score_data_keyword' does not exist."
                )
            })?;

        Ok(score_data_keyword.data)
    }

    pub(super) async fn find_query_result_vec<T: DeserializeOwned>(
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

    pub(super) async fn find_index_name_by_alias(
        &self,
        alias: &str,
    ) -> anyhow::Result<Vec<String>> {
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

    #[allow(dead_code)]
    pub(super) async fn find_consume_type_judgement(
        &self,
        product_name: &str,
    ) -> Result<ConsumeKeywordTypeResult, anyhow::Error> {
        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!(
                "[ElasticServiceImpl::find_consume_type_judgement] app_config: {:#}",
                e
            );
        })?;
        let es_spent_type: &str = app_config.es_spent_type_index_name().as_str();

        let es_query: Value = json!({
            "query": {
                "match": {
                    "consume_keyword": product_name
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

        let results: Vec<DocumentWithId<ConsumeKeywordTypeResult>> = self
            .find_query_result_vec(&response_body)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type_judgement] results: {:?}",
                    e
                )
            })?;

        Self::find_consume_type(product_name, results)
    }

    pub(super) async fn find_consume_type_judgements(
        &self,
        product_names: &[String],
    ) -> Result<Vec<ConsumeKeywordTypeResult>, anyhow::Error> {
        if product_names.is_empty() {
            return Ok(Vec::new());
        }

        let app_config: &AppConfig = AppConfig::get_global().inspect_err(|e| {
            error!(
                "[ElasticServiceImpl::find_consume_type_judgements] app_config: {:#}",
                e
            );
        })?;
        let es_spent_type_index: &str = app_config.es_spent_type_index_name().as_str();

        let es_queries: Vec<Value> = product_names
            .iter()
            .map(|product_name| {
                json!({
                    "query": {
                        "match": {
                            "consume_keyword": product_name
                        }
                    }
                })
            })
            .collect();

        let response_bodies: Vec<Value> = self
            .elastic_conn
            .find_all_by_queries(&es_queries, es_spent_type_index)
            .await
            .map_err(|e| {
                anyhow!(
                    "[ElasticServiceImpl::find_consume_type_judgements] response_body: {:?}",
                    e
                )
            })?;

        let mut consume_types: Vec<ConsumeKeywordTypeResult> =
            Vec::with_capacity(response_bodies.len());

        for (product_name, response_body) in product_names.iter().zip(response_bodies.iter()) {
            let results: Vec<DocumentWithId<ConsumeKeywordTypeResult>> = self
                .find_query_result_vec(response_body)
                .await
                .map_err(|e| {
                    anyhow!(
                        "[ElasticServiceImpl::find_consume_type_judgements] results: {:?}",
                        e
                    )
                })?;

            consume_types.push(Self::find_consume_type(product_name, results)?);
        }

        Ok(consume_types)
    }

    pub(super) async fn find_grouped_docs_with_range_agg<
        T: Send + Sync + DeserializeOwned,
    >(
        &self,
        query_options: GroupAggregationRangeQuery<'_>,
    ) -> Result<AggResultSet<T>, anyhow::Error> {
        let order_by_asc: &str = if query_options.ascending { "asc" } else { "desc" };

        let query: Value = json!({
            "size": query_options.query_size,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                query_options.range_field: {
                                    query_options.start_operator.to_str(): query_options.start_date.to_rfc3339(),
                                    query_options.end_operator.to_str(): query_options.end_date.to_rfc3339()
                                }
                            }
                        },
                        {
                            "term": {
                                "agg_group_seq": query_options.group_seq
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "aggs_result": {
                    "sum": {
                        "field": query_options.aggregation_field
                    }
                }
            },
            "sort": {
                query_options.order_by_field: { "order": order_by_asc }
            }
        });

        let response_body: Value = self
            .elastic_conn
            .find_by_query(&query, query_options.index_name)
            .await?;

        let agg_result: f64 = match &response_body["aggregations"]["aggs_result"]["value"].as_f64()
        {
            Some(agg_result) => *agg_result,
            None => {
                return Err(anyhow!(
                    "[Error][find_grouped_docs_with_range_agg()] 'agg_result' error"
                ));
            }
        };

        let consume_list: Vec<DocumentWithId<T>> =
            self.find_query_result_vec(&response_body).await?;

        Ok(AggResultSet::new(agg_result, consume_list))
    }
}
