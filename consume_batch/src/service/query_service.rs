use crate::common::*;

use crate::models::consume_prodt_detail::*;
use crate::models::consume_prodt_keyword::*;

use crate::repository::mysql_repository::*;

use crate::schema::{
    CONSUME_PRODT_KEYWORD, 
    CONSUMUE_KEYWORD_TYPE,
    CONSUME_PRODT_DETAIL
    };

use crate::schema::CONSUME_PRODT_DETAIL::dsl::*;

pub trait QueryService {
    fn consume_keyword_type_join_consume_prodt_keyword(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error>;
    fn get_top_consume_prodt_detail_order_by_timestamp(&self, top_n: i64, ascending: bool) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error>;
}   

#[derive(Debug, new)]   
pub struct QueryServicePub;


//impl<Expr: diesel::Expression> QueryService for QueryServicePub {

impl QueryService for QueryServicePub {

    #[doc = ""]
    fn consume_keyword_type_join_consume_prodt_keyword(&self) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {

        let mut conn = get_mysql_pool()?;
        // let pool = get_mysql_pool();
        // let mut conn =  pool.get()?;

        let result = CONSUME_PRODT_KEYWORD::table  
            .inner_join(CONSUMUE_KEYWORD_TYPE::table.on(CONSUME_PRODT_KEYWORD::consume_keyword_type.eq(CONSUMUE_KEYWORD_TYPE::consume_keyword_type)))
            .select((CONSUME_PRODT_KEYWORD::consume_keyword_type, CONSUME_PRODT_KEYWORD::consume_keyword))
            .load::<ConsumeProdtKeyword>(&mut conn)?;     

        Ok(result)
    }
    
    #[doc = ""]
    fn get_top_consume_prodt_detail_order_by_timestamp(&self, top_n: i64, ascending: bool) -> Result<Vec<ConsumeProdtDetail>, anyhow::Error> {

        let mut conn = get_mysql_pool()?;
        let result;

        if ascending {
            result = LimitDsl::limit(OrderDsl::order(CONSUME_PRODT_DETAIL, timestamp.asc()), top_n).load::<ConsumeProdtDetail>(&mut conn)?; 
        } else {
            result = LimitDsl::limit(OrderDsl::order(CONSUME_PRODT_DETAIL, timestamp.desc()), top_n).load::<ConsumeProdtDetail>(&mut conn)?; 
        }
        
        Ok(result)
    }
}


// p: disambiguate the method for candidate #1
//    |
// 47 |         let test = diesel::QueryDsl::limit(OrderDsl::order(CONSUME_PRODT_DETAIL, timestamp.asc()), top_n).load::<ConsumeProdtDetail>(conn)
//    |                    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// help: disambiguate the method for candidate #2
//    |
// 47 |         let test = diesel::query_dsl::methods::LimitDsl::limit(OrderDsl::order(CONSUME_PRODT_DETAIL, timestamp.asc()), top_n).load::<ConsumeProdtDetail>(conn)