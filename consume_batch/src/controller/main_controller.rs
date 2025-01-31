use crate::common::*;

use crate::service::es_query_service::*;
use crate::service::query_service::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_detail_es::*;
use crate::models::consume_prodt_keyword::*;
use crate::models::document_with_id::*;

use crate::configuration::elasitc_index_name::*;

#[derive(Debug, new)]
pub struct MainController<Q: QueryService, E: EsQueryService> {
    query_service: Q,
    es_query_service: E,
}

impl<Q: QueryService, E: EsQueryService> MainController<Q, E> {
    #[doc = "Elasticsearch data is loaded into MySQL through the ETL process. -> There is no data in the table "]
    async fn insert_es_to_mysql_empty_data(&self) -> Result<(), anyhow::Error> {
        /* Get all the data from elasticsearch and bulk insert it into MySQL. */
        let all_es_data: Vec<DocumentWithId<ConsumeProdtDetailES>> = self
            .es_query_service
            .get_all_list_from_es_partial::<ConsumeProdtDetailES>(&CONSUME_DETAIL)
            .await?;

        let all_es_to_rdb_data: Vec<ConsumeProdtDetail> = match all_es_data
            .into_iter()
            .map(|elem| elem.source.transfer_to_consume_prodt_detail())
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
        } else {
            info!("ES -> MySQL : {}", insert_size);
        }

        Ok(())
    }

    #[doc = "Elasticsearch data is loaded into MySQL through the ETL process. -> If there is data in the table"]
    /// # Arguments
    /// * `recent_prodt` - Most up-to-date data stored in the table
    ///
    /// # Returns
    /// * Result<(), anyhow::Error>
    async fn insert_es_to_mysql_non_empty_data(
        &self,
        recent_prodt: Vec<ConsumeProdtDetail>,
    ) -> Result<(), anyhow::Error> {
        let cur_timestamp: NaiveDateTime = recent_prodt
            .get(0)
            .ok_or_else(|| anyhow!("[Error][dynamic_indexing()] The 0th data in array 'recent_prodt' does not exist."))?
            .cur_timestamp;

        /* Get data after 'cur_timestamp' from Elasticsearch. */
        let es_recent_prodt_infos: Vec<DocumentWithId<ConsumeProdtDetailES>> = self
            .es_query_service
            .get_timetamp_gt_filter_list_from_es_partial(&CONSUME_DETAIL, cur_timestamp)
            .await?;

        /* If there are no changes */
        if es_recent_prodt_infos.is_empty() {
            info!("[dynamic_indexing()] There are no additional incremental indexes.");
            return Ok(());
        }

        let es_recent_prodt_infos_len: usize = es_recent_prodt_infos.len();
        /*
            If there are changes - put all the changed es data into MySQL here.
        */
        let consume_prodt_details: Vec<ConsumeProdtDetail> = match es_recent_prodt_infos
            .into_iter()
            .map(|elem| elem.source.transfer_to_consume_prodt_detail())
            .collect()
        {
            Ok(consume_prodt_details) => consume_prodt_details,
            Err(e) => {
                return Err(anyhow!("[Error][insert_es_to_mysql_non_empty_data()] Problem while converting vector 'consume_prodt_details' : {:?}", e));
            }
        };

        let insert_size: usize = insert_multiple_consume_prodt_detail(&consume_prodt_details)?;

        if insert_size != es_recent_prodt_infos_len {
            return Err(anyhow!("[Error][insert_es_to_mysql_non_empty_data()] The number of extracted data does not match the number of loaded data."));
        } else {
            info!("ES -> MySQL : {}", insert_size);
        }

        Ok(())
    }

    #[doc = "Elasticsearch data is loaded into MySQL through the ETL process."]
    async fn insert_batch_es_to_mysql(&self) -> Result<(), anyhow::Error> {
        /* Returns the most up-to-date written data that exists in MySQL
        -> If there is none, it can be considered that there is just no data. */
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

    #[doc = "Main Batch Function"]
    pub async fn main_task(&self) -> Result<(), anyhow::Error> {
        /* 1. ES -> MySQL ETL */
        self.insert_batch_es_to_mysql().await?;

        /* 2. MySQL -> ES Indexing */
        /* 2-1. consuming_index_prod_type */
        let consume_prodt_type: Vec<ConsumeProdtKeyword> =
            self.query_service.get_all_consume_prodt_type()?;

        self.es_query_service
            .post_indexing_data_by_bulk::<ConsumeProdtKeyword>(
                &CONSUME_TYPE,
                &CONSUME_TYPE_SETTINGS,
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
                &CONSUME_DETAIL,
                &CONSUME_DETAIL_SETTINGS,
                &consume_prodt_details,
            )
            .await?;

        Ok(())
    }
}
