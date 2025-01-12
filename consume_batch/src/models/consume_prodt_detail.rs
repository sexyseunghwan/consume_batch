use crate::common::*;

use crate::repository::mysql_repository::*;

use crate::schema::CONSUME_PRODT_DETAIL;
use crate::schema::CONSUME_PRODT_DETAIL::dsl::*;

#[derive(Queryable, Debug, Insertable, AsChangeset, Getters)]
#[table_name = "CONSUME_PRODT_DETAIL"]
#[getset(get = "pub")]
pub struct ConsumeProdtDetail {
    pub timestamp: NaiveDateTime,
    pub cur_timestamp: NaiveDateTime,
    pub prodt_name: String,
    pub prodt_money: i32,
    pub reg_dt: Option<NaiveDateTime>,
    pub chg_dt: Option<NaiveDateTime>,
    pub reg_id: Option<String>,
    pub chg_id: Option<String>,
}

#[doc = "Function that inserts 'ConsumeProdtDetail' object into MySQL - bulk insert"]
/// # Arguments
/// * `consume_prodt_detais` - Object of `ConsumeProdtDetail`
///
/// # Returns
/// * Result<usize, anyhow::Error>
pub fn insert_multiple_consume_prodt_detail(
    consume_prodt_detais: &Vec<ConsumeProdtDetail>,
) -> Result<usize, anyhow::Error> {
    let mut conn = get_mysql_pool()?;

    let size = diesel::insert_into(CONSUME_PRODT_DETAIL::table)
        .values(consume_prodt_detais)
        .execute(&mut conn)?;

    Ok(size)
}
