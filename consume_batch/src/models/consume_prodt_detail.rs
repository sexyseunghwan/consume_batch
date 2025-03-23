use crate::common::*;

use crate::repository::mysql_repository::*;

#[derive(Debug, FromQueryResult, Getters)]
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
