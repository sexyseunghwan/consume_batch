use crate::common::*;

use crate::repository::mysql_repository::*;

use crate::schema::CONSUME_PRODT_KEYWORD;
use crate::schema::CONSUME_PRODT_KEYWORD::dsl::*;

#[derive(Queryable, Serialize, Deserialize, Debug, Insertable, Getters)]
#[table_name = "CONSUME_PRODT_KEYWORD"]
#[getset(get = "pub")]
pub struct ConsumeProdtKeyword {
    pub consume_keyword_type: String,
    pub consume_keyword: String,
}

#[doc = ""]
pub fn get_consume_prodt_keyword_fileter_by_keyword_type(
    keyword: &str,
) -> Result<Vec<ConsumeProdtKeyword>, anyhow::Error> {
    let mut conn = get_mysql_pool()?;

    let result = QueryDsl::filter(CONSUME_PRODT_KEYWORD, consume_keyword_type.eq(keyword))
        .load::<ConsumeProdtKeyword>(&mut conn)?; /* Dereference Arc to get the connection */

    Ok(result)
}

pub fn insert_multiple_consume_prodt_keyword(
    consume_prodt_keyword: &Vec<ConsumeProdtKeyword>,
) -> Result<usize, anyhow::Error> {
    let mut conn = get_mysql_pool()?;

    let size = diesel::insert_into(CONSUME_PRODT_KEYWORD::table)
        .values(consume_prodt_keyword)
        .execute(&mut conn)?;

    Ok(size)
}
