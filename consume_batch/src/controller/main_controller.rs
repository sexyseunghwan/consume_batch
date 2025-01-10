use crate::common::*;

use crate::repository::es_repository::*;

use crate::service::es_query_service::*;
use crate::service::query_service::*;

use crate::utils_module::time_utils::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;

#[derive(Debug, new)]
pub struct MainController<Q: QueryService, E: EsQueryService> {
    query_service: Q,
    es_query_service: E,
}

impl<Q: QueryService, E: EsQueryService> MainController<Q, E> {
    #[doc = ""]
    // pub async fn migration_elastic_to_rdb_test(&self) -> Result<(), anyhow::Error> {

    //     let consume_details: Vec<ConsumeProdtDetailES> = self.query_service.get_all_consume_detail_list_from_es().await?;
    //     let mut parsing_rdb_consume_details: Vec<NewConsumeProdtDetail> = Vec::new();

    //     let formats = [
    //         "%Y-%m-%dT%H:%M:%S%.fZ",
    //         "%Y-%m-%dT%H:%M:%S%Z",
    //         "%Y-%m-%dT%H:%M:%S%.f",
    //         "%Y-%m-%dT%H:%M:%S",
    //         "%Y-%m-%dT%H:%M"
    //     ];

    //     for elem in consume_details {

    //         let mut timestamp = NaiveDateTime::parse_from_str("1970-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")?;

    //         for format in formats {
    //             if let Ok(parsed) = NaiveDateTime::parse_from_str(elem.timestamp(), format) {
    //                 timestamp = parsed;
    //                 break;
    //             }
    //         }

    //         //let timestamp = get_naive_datetime_from_str(elem.timestamp().split('.').next().unwrap_or(""), "%Y-%m-%dT%H:%M:%SZ")?;
    //         let cur_timestamp: NaiveDateTime;
    //         let prodt_name = elem.prodt_name().clone();
    //         let prodt_money = elem.prodt_money().clone();

    //         if let Some(value) = elem.cur_timestamp() {
    //             cur_timestamp = get_naive_datetime_from_str(value, "%Y-%m-%dT%H:%M:%SZ")?;
    //         } else {
    //             cur_timestamp = timestamp.clone();
    //         }

    //         let consume_prodt_detail = NewConsumeProdtDetail {
    //             timestamp: timestamp,
    //             cur_timestamp: cur_timestamp,
    //             prodt_name: prodt_name,
    //             prodt_money: prodt_money
    //         };

    //         parsing_rdb_consume_details.push(consume_prodt_detail);
    //     }

    //     let query_size = insert_multiple_consume_prodt_detail(parsing_rdb_consume_details)?;

    //     println!("query_size: {}", query_size);

    //     Ok(())
    // }
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

        //es_conn.clear_scroll_info(&scroll_id).await?;

        //.ok_or("No scroll_id found")?.to_string();

        // ==================================== TEST ====================================

        // let total_count_consume_detail = self.query_service.get_total_count_consume_prodt_detail()?;

        // if total_count_consume_detail == 0 {
        //     self.insert_es_to_mysql_empty_data().await?;
        // } else {
        //     self.insert_es_to_mysql_non_empty_data().await?;
        // }

        // RDB 에서 가장 뒤의 데이터를 가져와준다. -> 하나도 없다면, 데이터가 그냥 없다고 볼 수 있다.
        // let recent_prodt: Vec<ConsumeProdtDetail> = self
        //     .query_service
        //     .get_top_consume_prodt_detail_order_by_timestamp(1, false)?;

        // if recent_prodt.len() == 0 {
        // } else {
        // }

        Ok(())
    }

    #[doc = ""]
    pub fn first_task(&self) -> Result<(), anyhow::Error> {
        //let test = self.query_service.consume_keyword_type_join_consume_prodt_keyword()?;

        // for elem in test {
        //     println!("{:?}", elem);
        // }

        Ok(())
    }
}
