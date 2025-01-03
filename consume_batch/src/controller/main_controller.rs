use crate::common::*;

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
    pub fn dynamic_indexing(&self) -> Result<(), anyhow::Error> {
        // RDB 에서 가장 뒤의 데이터를 가져와준다.
        let recent_prodt = self
            .query_service
            .get_top_consume_prodt_detail_order_by_timestamp(1, false)?;

        if recent_prodt.len() == 0 {
            return Err(anyhow!(
                "[Error][dynamic_indexing()] Size 'recent_prodt' is 0."
            ));
        }

        //println!("{:?}", test);

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
