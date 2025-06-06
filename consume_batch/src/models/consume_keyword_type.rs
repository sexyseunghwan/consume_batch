use crate::common::*;

#[doc = "Structures to map to the `CONSUMUE_KEYWORD_TYPE` table"]
#[derive(Debug, FromQueryResult)]
pub struct ConsumeKeywordType {
    pub consume_keyword_type: String,
}

// #[derive(Queryable, Serialize, Deserialize, Debug, Insertable)]
// #[table_name = "CONSUMUE_KEYWORD_TYPE"]
// pub struct ConsumeKeywordType {
//     pub consume_keyword_type: String,
// }

// #[doc = ""]
// pub fn get_consume_prodt_keyword_fileter_by_keyword_type(keyword: &str) -> Result<Vec<ConsumeKeywordType>, anyhow::Error> {

//     let pool = get_mysql_pool();
//     let mut conn =  pool.get()?;

//     let result = QueryDsl::filter(CONSUMUE_KEYWORD_TYPE, consume_keyword_type.eq(keyword))
//         .load::<ConsumeKeywordType>(&mut conn)?; /* Dereference Arc to get the connection */
//     Ok(result)
// }
