// use crate::common::*;

// use crate::repository::mysql_repository::*;

// use crate::schema::CONSUME_PRODT_KEYWORD;
// use crate::schema::CONSUME_PRODT_KEYWORD::dsl::*;

// #[derive(Queryable, Serialize, Deserialize, Debug)]
// pub struct ConsumeProdtKeyword {
//     pub consume_keyword_type: String,
//     pub consume_keyword: String
// }


// #[doc = ""]
// pub fn get_consume_prodt_keyword_fileter_by_keyword_type(keyword: &str) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
    
//     let pool = get_mysql_pool();
//     let mut conn =  pool.get()?;
    
//     let result = QueryDsl::filter(CONSUME_PRODT_KEYWORD, consume_keyword_type.eq(keyword))
//         .load::<ConsumeProdtKeyword>(&mut conn)?; /* Dereference Arc to get the connection */ 
    
//     Ok(result)
// }




// #[derive(Insertable)]
// #[table_name="CONSUME_PRODT_KEYWORD"]
// pub struct NewConsumeProdtKeyword<'a> {
//     pub consume_keyword_type: &'a str,
//     pub consume_keyword: &'a str
// }