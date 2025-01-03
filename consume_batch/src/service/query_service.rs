use crate::common::*;

use crate::models::consume_prodt_keyword::*;
use crate::models::consume_prodt_detail_es::*;

use crate::repository::mysql_repository::*;
use crate::repository::es_repository::*;

use crate::schema::{
    CONSUME_PRODT_KEYWORD, 
    CONSUMUE_KEYWORD_TYPE,
    CONSUME_PRODT_DETAIL
    };



#[async_trait]
pub trait QueryService {
    fn consume_keyword_type_join_consume_prodt_keyword(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
    async fn get_all_consume_detail_list_from_es(&self) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error>;
}


#[derive(Debug, new)]
pub struct QueryServicePub;

#[async_trait]
impl QueryService for QueryServicePub {

    #[doc = ""]
    fn consume_keyword_type_join_consume_prodt_keyword(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {

        let pool = get_mysql_pool();
        let mut conn =  pool.get()?;

        let result = CONSUME_PRODT_KEYWORD::table  
            .inner_join(CONSUMUE_KEYWORD_TYPE::table.on(CONSUME_PRODT_KEYWORD::consume_keyword_type.eq(CONSUMUE_KEYWORD_TYPE::consume_keyword_type)))
            .select((CONSUME_PRODT_KEYWORD::consume_keyword_type, CONSUME_PRODT_KEYWORD::consume_keyword))
            .load::<ConsumeProdtKeyword>(&mut conn)?;     

        Ok(result)
    }
    

    #[doc = ""]
    async fn get_all_consume_detail_list_from_es(&self) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {
        
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

}