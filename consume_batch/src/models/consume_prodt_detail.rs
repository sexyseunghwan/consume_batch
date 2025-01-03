use crate::common::*;

use crate::repository::mysql_repository::*;

use crate::schema::CONSUME_PRODT_DETAIL;
use crate::schema::CONSUME_PRODT_DETAIL::dsl::*;

#[derive(Queryable, Debug)]
pub struct ConsumeProdtDetail {
    pub timestamp: NaiveDateTime,
    pub cur_timestamp: NaiveDateTime,
    pub prodt_name: String,
    pub prodt_money: i32
}


#[derive(Insertable)]
#[table_name="CONSUME_PRODT_DETAIL"]
pub struct NewConsumeProdtDetail {
    pub timestamp: NaiveDateTime,
    pub cur_timestamp: NaiveDateTime,
    pub prodt_name: String,
    pub prodt_money: i32,
}

#[doc = ""]
pub fn insert_multiple_consume_prodt_detail(consume_prodt_detais: Vec<NewConsumeProdtDetail>) -> Result<usize, anyhow::Error> {

    let pool = get_mysql_pool();
    let mut conn =  pool.get()?;

    let size = diesel::insert_into(CONSUME_PRODT_DETAIL::table)
        .values(&consume_prodt_detais) 
        .execute(&mut conn)?;

    Ok(size)
}