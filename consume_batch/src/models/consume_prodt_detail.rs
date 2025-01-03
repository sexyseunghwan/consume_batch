use crate::common::*;

use crate::repository::mysql_repository::*;

use crate::schema::CONSUME_PRODT_DETAIL;
use crate::schema::CONSUME_PRODT_DETAIL::dsl::*;

#[derive(Queryable, Debug, Insertable)]
#[table_name = "CONSUME_PRODT_DETAIL"]
pub struct ConsumeProdtDetail {
    pub timestamp: NaiveDateTime,
    pub cur_timestamp: NaiveDateTime,
    pub prodt_name: String,
    pub prodt_money: i32,
}

#[doc = ""]
pub fn insert_multiple_consume_prodt_detail(
    consume_prodt_detais: Vec<ConsumeProdtDetail>,
) -> Result<usize, anyhow::Error> {
    let mut conn = get_mysql_pool()?;

    let size = diesel::insert_into(CONSUME_PRODT_DETAIL::table)
        .values(&consume_prodt_detais)
        .execute(&mut conn)?;

    Ok(size)
}
