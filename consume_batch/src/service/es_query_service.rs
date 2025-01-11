use crate::common::*;

use crate::repository::es_repository::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;

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
        es_conn.bulk_query(&new_index_name, data).await?;

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

    #[doc = ""]
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

    #[doc = ""]
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
}
