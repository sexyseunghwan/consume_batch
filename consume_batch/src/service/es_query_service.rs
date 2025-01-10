use serde::ser::Error;

use crate::common::*;

use crate::repository::es_repository::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;

use crate::utils_module::time_utils::*;

#[async_trait]
pub trait EsQueryService {
    // async fn get_all_consume_detail_list_from_es(
    //     &self,
    // ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;

    // async fn get_consume_detail_list_gte_cur_timstamp_from_es(
    //     &self,
    //     timestamp: NaiveDateTime,
    // ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;

    // async fn get_consume_detail_list_from_es_partial(
    //     &self,
    //     query: &Value,
    //     scroll_id: &str,
    // ) -> Result<(String, Vec<ConsumeProdtDetailES>), anyhow::Error>;

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

    // async fn get_transfer_indexingtype_consume_detail_list(
    //     &self,
    //     consume_detail_list: &Vec<ConsumeProdtDetail>,
    // ) -> Result<ConsumeProdtDetailES, anyhow::Error>;
    // async fn get_consume_specific_datetime_detail_list_from_es_partial(
    //     &self,
    //     scroll_id: &str,
    // ) -> Result<(String, Vec<ConsumeProdtDetailES>), anyhow::Error>;
}

#[derive(Debug, new)]
pub struct EsQueryServicePub;

#[async_trait]
impl EsQueryService for EsQueryServicePub {
    // #[doc = ""]
    // async fn get_transfer_indexingtype_consume_detail_list(
    //     &self,
    //     consume_detail_list: &Vec<ConsumeProdtDetail>,
    // ) -> Result<ConsumeProdtDetailES, anyhow::Error> {
    // }

    #[doc = ""]
    async fn get_timetamp_gt_filter_list_from_es_partial<T: for<'de> Deserialize<'de> + Send>(
        &self,
        index_name: &str,
        start_dt: NaiveDateTime,
    ) -> Result<Vec<T>, anyhow::Error> {
        info!("start_dt: {:?}", start_dt);

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

        info!("query: {:?}", query);

        let res = self
            .get_search_data_by_bulk::<T>(index_name, &query)
            .await?;

        Ok(res)
    }

    #[doc = ""]
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

        Ok(hits_vector)
    }

    // #[doc = ""]
    // async fn get_consume_specific_datetime_detail_list_from_es_partial(
    //     &self,
    //     scroll_id: &str,
    // ) -> Result<(String, Vec<ConsumeProdtDetailES>), anyhow::Error> {
    //     let query = json!({
    //         "query": {
    //             "match_all": {}
    //         },
    //         "size": 1000
    //     });

    //     let result = self
    //         .get_consume_detail_list_from_es_partial(&query, scroll_id)
    //         .await?;

    //     Ok(result)
    // }

    // #[doc = "이것도 뭔가 좀 그런데 그저 재료일뿐으로 생각해야 되겠는걸"]
    // async fn get_consume_all_detail_list_from_es_partial(
    //     &self,
    //     scroll_id: &str,
    // ) -> Result<(String, Vec<ConsumeProdtDetailES>), anyhow::Error> {
    //     let query = json!({
    //         "query": {
    //             "match_all": {}
    //         },
    //         "size": 1000
    //     });

    //     let result = self
    //         .get_consume_detail_list_from_es_partial(&query, scroll_id)
    //         .await?;

    //     Ok(result)
    // }

    // #[doc = ""]
    // async fn get_consume_detail_list_from_es_partial(
    //     &self,
    //     query: &Value,
    //     scroll_id: &str,
    // ) -> Result<(String, Vec<ConsumeProdtDetailES>), anyhow::Error> {
    //     let es_conn = get_elastic_conn()?;

    //     if scroll_id == "" {
    //         let scroll_resp: Value = es_conn
    //             .get_scroll_initial_search_query(CONSUME_DETAIL, "1m", &query)
    //             .await?;

    //         let scroll_id = scroll_resp
    //             .get("_scroll_id")
    //             .map_or(String::from(""), |s| s.to_string())
    //             .trim_matches('"')
    //             .replace(r#"\""#, "");

    //         let hits = &scroll_resp["hits"]["hits"];

    //         let results: Vec<ConsumeProdtDetailES> = hits.as_array()
    //             .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es_partial()] error"))?
    //             .iter()
    //             .map(|hit| {
    //                 hit.get("_source")
    //                     .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es_partial()] Missing '_source' field"))
    //                     .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
    //             })
    //             .collect::<Result<Vec<_>, _>>()?;

    //         Ok((scroll_id, results))
    //     } else {
    //         let scroll_resp = es_conn.get_scroll_search_query("1m", scroll_id).await?;

    //         let scroll_id = scroll_resp
    //             .get("_scroll_id")
    //             .map_or(String::from(""), |s| s.to_string())
    //             .trim_matches('"')
    //             .replace(r#"\""#, "");

    //         let hits = &scroll_resp["hits"]["hits"]
    //             .as_array()
    //             .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es_partial()] There was a problem converting the 'hits' variable into an array."))?;

    //         if hits.is_empty() {
    //             let vec: Vec<ConsumeProdtDetailES> = Vec::new();
    //             return Ok((String::from(""), vec));
    //         }

    //         let results: Vec<ConsumeProdtDetailES> = hits
    //             .iter()
    //             .map(|hit| {
    //                 hit.get("_source")
    //                     .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es_partial()] Missing '_source' field"))
    //                     .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
    //             })
    //             .collect::<Result<Vec<_>, _>>()?;

    //         Ok((scroll_id, results))
    //     }
    // }

    // #[doc = "deprecated"]
    // async fn get_all_consume_detail_list_from_es(
    //     &self,
    // ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
    //     let es_client = get_elastic_conn()?;

    //     let query = json!({
    //         "size": 10000
    //     });

    //     let response_body = es_client.get_search_query(&query, CONSUME_DETAIL).await?;
    //     let hits = &response_body["hits"]["hits"];

    //     let results: Vec<ConsumeProdtDetailES> = hits.as_array()
    //         .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es()] error"))?
    //         .iter()
    //         .map(|hit| {
    //             hit.get("_source")
    //                 .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es()] Missing '_source' field"))
    //                 .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
    //         })
    //         .collect::<Result<Vec<_>, _>>()?;

    //     Ok(results)
    // }

    // #[doc = ""]
    // async fn get_consume_detail_list_gte_cur_timstamp_from_es(
    //     &self,
    //     timestamp: NaiveDateTime,
    // ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
    //     let es_client = get_elastic_conn()?;
    //     let timestamp_str = get_str_from_naive_datetime(timestamp);

    //     let query = json!({
    //         "query": {
    //             "bool": {
    //               "must": [
    //                 {
    //                   "range": {
    //                     "cur_timestamp": {
    //                       "gt": timestamp_str
    //                     }
    //                   }
    //                 }
    //               ]
    //             }
    //           }
    //     });

    //     let response_body = es_client.get_search_query(&query, CONSUME_DETAIL).await?;
    //     let hits = &response_body["hits"]["hits"];

    //     let results: Vec<ConsumeProdtDetailES> = hits.as_array()
    //         .ok_or_else(|| anyhow!("[Error][get_consume_detail_list_gte_cur_timstamp_from_es()] error"))?
    //         .iter()
    //         .map(|hit| {
    //             hit.get("_source")
    //                 .ok_or_else(|| anyhow!("[Error][get_consume_detail_list_gte_cur_timstamp_from_es()] Missing '_source' field"))
    //                 .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
    //         })
    //         .collect::<Result<Vec<_>, _>>()?;

    //     Ok(results)
    // }
}
