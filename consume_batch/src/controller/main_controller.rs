use crate::common::*;

use crate::service::query_service::*;

use crate::utils_module::time_utils::*;

use crate::models::consume_prodt_detail_es::*;
use crate::models::consume_prodt_detail::*;


#[derive(Debug, new)]
pub struct MainController<Q: QueryService> {
    query_service: Q
}

impl<Q: QueryService> MainController<Q> {


    // pub timestamp: NaiveDateTime,
    // pub cur_timestamp: NaiveDateTime,
    // pub prodt_name: &'a str,
    // pub prodt_money: &'a i32,

    #[doc = ""]
    pub async fn migration_elastic_to_rdb(&self) -> Result<(), anyhow::Error> {

        let consume_details: Vec<ConsumeProdtDetailES> = self.query_service.get_all_consume_detail_list_from_es().await?;
        let mut parsing_rdb_consume_details: Vec<NewConsumeProdtDetail> = Vec::new();
        
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.fZ",  
            "%Y-%m-%dT%H:%M:%S%Z",    
            "%Y-%m-%dT%H:%M:%S%.f",   
            "%Y-%m-%dT%H:%M:%S",      
        ];
        
        for elem in consume_details {
            
            let mut timestamp;

            for format in formats {
                if let Ok(parsed) = NaiveDateTime::parse_from_str(elem.timestamp(), format) {
                    timestamp = parsed;
                }
            }

            //let timestamp = get_naive_datetime_from_str(elem.timestamp().split('.').next().unwrap_or(""), "%Y-%m-%dT%H:%M:%SZ")?;
            let cur_timestamp: NaiveDateTime;
            let prodt_name = elem.prodt_name().clone();
            let prodt_money = elem.prodt_money().clone();
            
            if let Some(value) = &elem.cur_timestamp {
                cur_timestamp = get_naive_datetime_from_str(value, "%Y-%m-%dT%H:%M:%SZ")?;         
            } else {
                cur_timestamp = timestamp.clone();
            }
            
            let consume_prodt_detail = NewConsumeProdtDetail {
                timestamp: timestamp,
                cur_timestamp: cur_timestamp,
                prodt_name: prodt_name,
                prodt_money: prodt_money
            };

            parsing_rdb_consume_details.push(consume_prodt_detail);
        }
        
        let query_size = insert_multiple_consume_prodt_detail(parsing_rdb_consume_details)?;

        println!("query_size: {}", query_size);

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