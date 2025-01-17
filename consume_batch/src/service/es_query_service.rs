use crate::common::*;

use crate::repository::es_repository::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;
use crate::models::consume_prodt_keyword::*;
use crate::models::score_manager::*;

use crate::utils_module::io_utils::*;
use crate::utils_module::time_utils::*;

#[async_trait]
pub trait EsQueryService {
    async fn get_search_data_by_bulk<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
        query: &Value,
    ) -> Result<Vec<T>, anyhow::Error>;

    async fn get_all_list_from_es_partial<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
    ) -> Result<Vec<T>, anyhow::Error>;

    async fn get_timetamp_gt_filter_list_from_es_partial<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
        start_dt: NaiveDateTime,
    ) -> Result<Vec<T>, anyhow::Error>;

    async fn post_indexing_data_by_bulk<T: Serialize + Send + Sync>(
        &self,
        index_alias_name: &str,
        index_settings_path: &str,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error>;

    async fn get_consume_prodt_details_specify_type(
        &self,
        consume_prodt_details: &Vec<ConsumeProdtDetail>,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;
}

#[derive(Debug, new)]
pub struct EsQueryServicePub;

#[async_trait]
impl EsQueryService for EsQueryServicePub {
    #[doc = "static index function"]
    /// # Arguments
    /// * `index_alias_name` - alias for index
    /// * `index_settings_path` - File path for setting index schema
    /// * `data` - Vector information to be indexed
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn post_indexing_data_by_bulk<T: Serialize + Send + Sync>(
        &self,
        index_alias_name: &str,
        index_settings_path: &str,
        data: &Vec<T>,
    ) -> Result<(), anyhow::Error> {
        let es_conn = get_elastic_conn()?;

        /* Put today's date time on the index you want to create. */
        let curr_time = get_current_kor_naive_datetime()
            .format("%Y%m%d%H%M%S")
            .to_string();
        let new_index_name: String = format!("{}-{}", index_alias_name, curr_time);

        let json_body: Value = read_json_from_file(index_settings_path)?;
        es_conn.create_index(&new_index_name, &json_body).await?;

        /* Bulk post the data to the index above at once. */
        es_conn.bulk_indexing_query(&new_index_name, data).await?;

        /* Change alias */
        let alias_resp = es_conn
            .get_indexes_mapping_by_alias(index_alias_name)
            .await?;
        let old_index_name: String;
        if let Some(first_key) = alias_resp.as_object().and_then(|map| map.keys().next()) {
            old_index_name = first_key.to_string();
        } else {
            return Err(anyhow!("[Error][post_indexing_data_by_bulk()] Failed to extract index name within 'index-alias'"));
        }

        es_conn
            .update_index_alias(index_alias_name, &new_index_name, &old_index_name)
            .await?;
        es_conn.delete_query(&old_index_name).await?;

        /* Functions to enable search immediately after index */
        es_conn.refresh_index(index_alias_name).await?;

        Ok(())
    }

    #[doc = "Function that returns all data after a certain time in a particular index"]
    /// # Arguments
    /// * `index_name` - Name of index
    /// * `start_dt` - Start time
    ///
    /// # Returns
    /// * Result<Vec<T>, anyhow::Error>
    async fn get_timetamp_gt_filter_list_from_es_partial<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
        start_dt: NaiveDateTime,
    ) -> Result<Vec<T>, anyhow::Error> {
        let query = json!({
            "query": {
                "range": {
                    "@timestamp" : {
                        "gt": get_str_from_naive_datetime(start_dt)
                    }
                }
            },
            "size": 1000
        });

        let res = self
            .get_search_data_by_bulk::<T>(index_name, &query)
            .await?;

        Ok(res)
    }

    #[doc = "Function that partially returns all data in a particular index - elasticsearch can only view 10,000 times at a time."]
    /// # Arguments
    /// * `index_name` - Name of index
    ///
    /// # Returns
    /// * Result<Vec<T>, anyhow::Error>
    async fn get_all_list_from_es_partial<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
    ) -> Result<Vec<T>, anyhow::Error> {
        let query = json!({
            "query": {
                "match_all": {}
            },
            "size": 1000
        });

        let res_vec = self
            .get_search_data_by_bulk::<T>(index_name, &query)
            .await?;

        Ok(res_vec)
    }

    #[doc = "Function that returns all data of a particular index through a 'scroll api'"]
    /// # Arguments
    /// * `index_name` - Name of index
    /// * `query` - Queries to apply
    ///
    /// # Returns
    /// * Result<Vec<T>, anyhow::Error>
    async fn get_search_data_by_bulk<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
        query: &Value,
    ) -> Result<Vec<T>, anyhow::Error> {
        let es_conn = get_elastic_conn()?;

        let scroll_resp: Value = es_conn
            .get_scroll_initial_search_query(index_name, "1m", &query)
            .await?;

        let mut scroll_id = scroll_resp
            .get("_scroll_id")
            .map_or(String::from(""), |s| s.to_string())
            .trim_matches('"')
            .replace(r#"\""#, "");

        let hits = &scroll_resp["hits"]["hits"];

        let mut hits_vector: Vec<T> = hits
            .as_array()
            .ok_or_else(|| anyhow!("[Error][get_data_from_es_bulk()] error"))?
            .iter()
            .map(|hit| {
                hit.get("_source")
                    .ok_or_else(|| {
                        anyhow!("[Error][get_data_from_es_bulk()] Missing '_source' field")
                    })
                    .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
            })
            .collect::<Result<Vec<_>, _>>()?;

        loop {
            let scroll_resp = es_conn.get_scroll_search_query("1m", &scroll_id).await?;

            scroll_id = scroll_resp
                .get("_scroll_id")
                .map_or(String::from(""), |s| s.to_string())
                .trim_matches('"')
                .replace(r#"\""#, "");

            let scroll_resp_hits = &scroll_resp["hits"]["hits"]
                .as_array()
                .ok_or_else(|| anyhow!("[Error][get_data_from_es_bulk()] There was a problem converting the 'hits' variable into an array."))?;

            if scroll_resp_hits.is_empty() {
                break;
            }

            let scroll_resp_hits_vector: Vec<T> = scroll_resp_hits
                .iter()
                .map(|hit| {
                    hit.get("_source")
                        .ok_or_else(|| {
                            anyhow!("[Error][get_data_from_es_bulk()] Missing '_source' field")
                        })
                        .and_then(|source| {
                            serde_json::from_value(source.clone()).map_err(Into::into)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            hits_vector.extend(scroll_resp_hits_vector);
        }

        es_conn.clear_scroll_info(&scroll_id).await?;

        Ok(hits_vector)
    }

    #[doc = "Function that designates consumption classification of consumption-related data and converts data according to the index format"]
    /// # Arguments
    /// * `consume_prodt_details` - Consumption-related data vectors
    ///
    /// # Returns
    /// * Result<Vec<ConsumeProdtDetailES>, anyhow::Error>
    async fn get_consume_prodt_details_specify_type(
        &self,
        consume_prodt_details: &Vec<ConsumeProdtDetail>,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
        let es_conn: EsRepositoryPub = get_elastic_conn()?;
        let mut consume_prodt_details_es: Vec<ConsumeProdtDetailES> = Vec::new();

        for prodt_detail in consume_prodt_details {
            let prodt_name = prodt_detail.prodt_name.to_string();
            let prodt_type: String;
            let es_query = json!({
                "query": {
                    "match": {
                        "consume_keyword": prodt_name
                    }
                }
            });

            let search_res_body = es_conn.get_search_query(&es_query, CONSUME_TYPE).await?;
            let hits = &search_res_body["hits"]["hits"];

            let results: Vec<ConsumeProdtKeyword> = hits.as_array()
                .ok_or_else(|| anyhow!("[Error][get_consume_prodt_details_specify_type()] error"))?
                .iter()
                .map(|hit| {
                    hit.get("_source") 
                        .ok_or_else(|| anyhow!("[Error][get_consume_prodt_details_specify_type()] Missing '_source' field"))
                        .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if results.is_empty() {
                prodt_type = String::from("etc");
            } else {
                let mut manager: ScoreManager<ConsumeProdtKeyword> =
                    ScoreManager::<ConsumeProdtKeyword>::new();

                for consume_type in results {
                    let keyword = consume_type.consume_keyword();

                    /* Use the 'levenshtein' algorithm to determine word match */
                    let word_dist = levenshtein(keyword, &prodt_name);
                    let word_dist_i32: i32 = word_dist.try_into()?;
                    manager.insert(word_dist_i32, consume_type);
                }

                let score_data_keyword: ScoredData<ConsumeProdtKeyword> = match manager.pop_lowest()
                {
                    Some(score_data_keyword) => score_data_keyword,
                    None => {
                        error!("[Error][get_consume_prodt_details_specify_type()] The mapped data for variable 'score_data_keyword' does not exist.");
                        continue;
                    }
                };
                
                prodt_type = score_data_keyword.data().consume_keyword_type().to_string();
            }

            let prodt_detail_timestamp = get_str_from_naive_datetime(*prodt_detail.timestamp());
            let prodt_detail_cur_timestamp = get_str_from_naive_datetime(*prodt_detail.timestamp());

            let consume_detail_es = ConsumeProdtDetailES::new(
                prodt_detail_timestamp,
                Some(prodt_detail_cur_timestamp),
                prodt_detail.prodt_name().to_string(),
                *prodt_detail.prodt_money(),
                Some(prodt_type),
            );

            consume_prodt_details_es.push(consume_detail_es);
        }

        Ok(consume_prodt_details_es)
    }
}
