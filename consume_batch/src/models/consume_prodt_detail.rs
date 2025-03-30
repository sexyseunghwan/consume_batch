use crate::common::*;

use crate::entity::consume_prodt_detail;

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

#[doc = "Converting 'ConsumeProdtDetail' structure to Seaorm-compatible active model"]
/// # Arguments
/// * `data` - List of `ConsumeProdtDetail`
///
/// # Returns
/// * Vec<consume_prodt_detail::ActiveModel>
pub fn to_active_models_comsume_prodt_detail(
    data: Vec<ConsumeProdtDetail>,
) -> Vec<consume_prodt_detail::ActiveModel> {
    data.into_iter()
        .map(|item| consume_prodt_detail::ActiveModel {
            timestamp: ActiveValue::Set(item.timestamp),
            cur_timestamp: ActiveValue::Set(item.cur_timestamp),
            prodt_name: ActiveValue::Set(item.prodt_name),
            prodt_money: ActiveValue::Set(item.prodt_money),
            reg_dt: ActiveValue::Set(item.reg_dt.expect("[Error][to_active_models_comsume_prodt_detail()] There is a problem with the `reg_dt` value.")),
            chg_dt: ActiveValue::Set(item.chg_dt),
            reg_id: ActiveValue::Set(item.reg_id.expect("[Error][to_active_models_comsume_prodt_detail()]] There is a problem with the `reg_id` value.")),
            chg_id: ActiveValue::Set(item.chg_id),
            ..Default::default()
        })
        .collect()
}
