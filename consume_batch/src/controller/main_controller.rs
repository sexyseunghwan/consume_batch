use crate::common::*;

use crate::repository::es_repository::*;

use crate::service::es_query_service::*;
use crate::service::query_service::*;

use crate::utils_module::time_utils::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;
use crate::models::consume_prodt_keyword::*;
use crate::models::consume_prodt_keyword_es::*;

#[derive(Debug, new)]
pub struct MainController<Q: QueryService, E: EsQueryService> {
    query_service: Q,
    es_query_service: E,
}

impl<Q: QueryService, E: EsQueryService> MainController<Q, E> {
    #[doc = ""]
    pub async fn insert_es_to_mysql_empty_data(&self) -> Result<(), anyhow::Error> {
        /* elasticsearch 의 모든 데이터를 가져와서 MySQL 에 bulk insert 해준다. */
        let all_es_data: Vec<ConsumeProdtDetailES> = self
            .es_query_service
            .get_all_list_from_es_partial::<ConsumeProdtDetailES>(CONSUME_DETAIL)
            .await?;

        let all_es_to_rdb_data: Vec<ConsumeProdtDetail> = match all_es_data
            .into_iter()
            .map(|elem| elem.transfer_to_consume_prodt_detail())
            .collect()
        {
            Ok(all_es_to_rdb_data) => all_es_to_rdb_data,
            Err(e) => {
                return Err(anyhow!("[Error][insert_es_to_mysql_empty_data()] Problem while converting vector 'all_es_to_rdb_data' : {:?}", e));
            }
        };

        let insert_size: usize = insert_multiple_consume_prodt_detail(&all_es_to_rdb_data)?;

        if insert_size != all_es_to_rdb_data.len() {
            return Err(anyhow!("[Error][insert_es_to_mysql_empty_data()] The number of extracted data does not match the number of loaded data."));
        }

        Ok(())
    }

    #[doc = ""]
    pub async fn insert_es_to_mysql_non_empty_data(
        &self,
        recent_prodt: Vec<ConsumeProdtDetail>,
    ) -> Result<(), anyhow::Error> {
        let cur_timestamp: NaiveDateTime = recent_prodt
            .get(0)
            .ok_or_else(|| anyhow!("[Error][dynamic_indexing()] The 0th data in array 'recent_prodt' does not exist."))?
            .cur_timestamp;

        /* Elasticsearch 에서 cur_timestamp 이후의 데이터를 가져와준다. */
        let es_recent_prodt_infos: Vec<ConsumeProdtDetailES> = self
            .es_query_service
            .get_timetamp_gt_filter_list_from_es_partial(CONSUME_DETAIL, cur_timestamp)
            .await?;

        /* 변경분이 없는 경우 */
        if es_recent_prodt_infos.is_empty() {
            info!("[dynamic_indexing()] There are no additional incremental indexes.");
            return Ok(());
        }

        let es_recent_prodt_infos_len = es_recent_prodt_infos.len();
        /*
            변경분이 있는 경우 - 여기서 변경된 es 데이터를 모두 MySQL 에 넣어준다.
        */
        let consume_prodt_details: Vec<ConsumeProdtDetail> = match es_recent_prodt_infos
            .into_iter()
            .map(|elem| elem.transfer_to_consume_prodt_detail())
            .collect()
        {
            Ok(consume_prodt_details) => consume_prodt_details,
            Err(e) => {
                return Err(anyhow!("[Error][insert_es_to_mysql_non_empty_data()] Problem while converting vector 'consume_prodt_details' : {:?}", e));
            }
        };

        let insert_size = insert_multiple_consume_prodt_detail(&consume_prodt_details)?;

        if insert_size != es_recent_prodt_infos_len {
            return Err(anyhow!("[Error][insert_es_to_mysql_non_empty_data()] The number of extracted data does not match the number of loaded data."));
        }

        Ok(())
    }

    #[doc = ""]
    pub async fn insert_batch_es_to_mysql(&self) -> Result<(), anyhow::Error> {
        /* RDB 에서 가장 뒤의 데이터를 가져와준다. -> 하나도 없다면, 데이터가 그냥 없다고 볼 수 있다. */
        let recent_prodt: Vec<ConsumeProdtDetail> = self
            .query_service
            .get_top_consume_prodt_detail_order_by_timestamp(1, false)?;

        if recent_prodt.is_empty() {
            self.insert_es_to_mysql_empty_data().await?;
        } else {
            self.insert_es_to_mysql_non_empty_data(recent_prodt).await?;
        }

        Ok(())
    }

    #[doc = ""]
    pub async fn insert_batch_mysql_to_es(&self) -> Result<(), anyhow::Error> {
        // RDB 에 저장된 소비 데이터 모두를 가져와준다.
        let consume_detail_list: Vec<ConsumeProdtDetail> =
            self.query_service.get_all_consume_prodt_detail()?;

        // ES 관련 데이터로 변환 && 어떤 종류의 상품인지 분류 해주는 작업

        Ok(())
    }

    // ==================== [FOR TEST] ====================
    pub async fn insert_consume_type_to_mysql(&self) -> Result<(), anyhow::Error> {
        let es_conn = get_elastic_conn()?;

        let es_query = json!({
            "query": {
                "match_all": {}
            },
            "size": 1000
        });

        let search_resp = es_conn
            .get_search_query(&es_query, CONSUME_TYPE)
            .await?;
        
        let hits: &Value = &search_resp["hits"]["hits"];

        let hits_vector: Vec<ConsumeProdtKeywordES> = hits
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

        let mut cpk_vec: Vec<ConsumeProdtKeyword> = Vec::new();

        for consume_keyword in hits_vector {
            let consume_keyword_trs = ConsumeProdtKeyword {
                consume_keyword_type: consume_keyword.keyword_type,
                consume_keyword: consume_keyword.keyword,
            };

            cpk_vec.push(consume_keyword_trs);
        }

        let size = insert_multiple_consume_prodt_keyword(&cpk_vec)?;

        Ok(())
    }
    // ==================== [FOR TEST] ====================

    #[doc = "Main Batch Function"]
    pub async fn main_task(&self) -> Result<(), anyhow::Error> {
        /* 1. ES -> MySQL ETL */

        /* 2. MySQL -> ES Indexing */
        /* 2-1. consuming_index_prod_type */
        let consume_prodt_type: Vec<ConsumeProdtKeyword> =
            self.query_service.get_all_consume_prodt_type()?;

        self.es_query_service
            .post_indexing_data_by_bulk::<ConsumeProdtKeyword>(
                CONSUME_TYPE,
                CONSUME_TYPE_SETTINGS,
                &consume_prodt_type,
            )
            .await?;
        
        /* 2-2. consume_prodt_details */
        let consume_prodt_details: Vec<ConsumeProdtDetail> =
            self.query_service.get_all_consume_prodt_detail()?;

        let consume_prodt_details: Vec<ConsumeProdtDetailES> = self
            .es_query_service
            .get_consume_prodt_details_specify_type(&consume_prodt_details)
            .await?;

        self.es_query_service
            .post_indexing_data_by_bulk::<ConsumeProdtDetailES>(
                CONSUME_DETAIL,
                CONSUME_DETAIL_SETTINGS,
                &consume_prodt_details,
            )
            .await?;

        // let consume_prodt_type_es: Vec<ConsumeProdtKeywordES> = consume_prodt_type
        //     .iter()
        //     .map(|elem| elem.transfer_to_consume_prodt_keyword_es())
        //     .collect();

        /* 2-2. consuming_index_prod_new */

        //let test = self.query_service.consume_keyword_type_join_consume_prodt_keyword()?;

        // for elem in test {
        //     println!("{:?}", elem);
        // }

        Ok(())
    }
}
