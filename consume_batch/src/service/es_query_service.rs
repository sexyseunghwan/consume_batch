use crate::common::*;

use crate::repository::es_repository::*;

use crate::models::consume_prodt_detail_es::*;

use crate::utils_module::time_utils::*;

#[async_trait]
pub trait EsQueryService {
    async fn get_all_consume_detail_list_from_es(
        &self,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;

    async fn get_consume_detail_list_gte_cur_timstamp_from_es(
        &self,
        timestamp: NaiveDateTime,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;
}

#[derive(Debug, new)]
pub struct EsQueryServicePub;

#[async_trait]
impl EsQueryService for EsQueryServicePub {
    #[doc = ""]
    async fn get_all_consume_detail_list_from_es(
        &self,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
        let es_client = get_elastic_conn()?;

        let query = json!({
            "size": 10000
        });

        let response_body = es_client.get_search_query(&query, CONSUME_DETAIL).await?;
        let hits = &response_body["hits"]["hits"];

        let results: Vec<ConsumeProdtDetailES> = hits.as_array()
            .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es()] error"))?
            .iter()
            .map(|hit| {
                hit.get("_source") 
                    .ok_or_else(|| anyhow!("[Error][get_all_consume_detail_list_from_es()] Missing '_source' field"))
                    .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    #[doc = ""]
    async fn get_consume_detail_list_gte_cur_timstamp_from_es(
        &self,
        timestamp: NaiveDateTime,
    ) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
        let es_client = get_elastic_conn()?;
        let timestamp_str = get_str_from_naive_datetime(timestamp);

        let query = json!({
            "query": {
                "bool": {
                  "must": [
                    {
                      "range": {
                        "cur_timestamp": {
                          "gt": timestamp_str
                        }
                      }
                    }
                  ]
                }
              }
        });

        let response_body = es_client.get_search_query(&query, CONSUME_DETAIL).await?;
        let hits = &response_body["hits"]["hits"];

        let results: Vec<ConsumeProdtDetailES> = hits.as_array()
            .ok_or_else(|| anyhow!("[Error][get_consume_detail_list_gte_cur_timstamp_from_es()] error"))?
            .iter()
            .map(|hit| {
                hit.get("_source") 
                    .ok_or_else(|| anyhow!("[Error][get_consume_detail_list_gte_cur_timstamp_from_es()] Missing '_source' field"))
                    .and_then(|source| serde_json::from_value(source.clone()).map_err(Into::into))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }
}
