use crate::common::*;

use crate::models::consume_prodt_detail::*;

use crate::utils_module::time_utils::*;

#[derive(Debug, Getters, Serialize, Deserialize, new)]
#[getset(get = "pub")]
pub struct ConsumeProdtDetailES {
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    pub cur_timestamp: Option<String>,
    pub prodt_name: String,
    pub prodt_money: i32,
}

//ConsumeProdtDetail

impl ConsumeProdtDetailES {
    #[doc = "Functions that convert elasticsearch schema to schema data formats in MySQL format"]
    pub fn transfer_to_consume_prodt_detail(&self) -> Result<ConsumeProdtDetail, anyhow::Error> {
        let timestamp = get_naive_datetime_from_str(self.timestamp(), "%Y-%m-%dT%H:%M:%SZ")?;
        let cur_timestamp;

        if let Some(value) = self.cur_timestamp() {
            cur_timestamp = get_naive_datetime_from_str(value, "%Y-%m-%dT%H:%M:%SZ")?;
        } else {
            cur_timestamp = timestamp.clone();
        }

        let consume_prodt_detail = ConsumeProdtDetail {
            timestamp: timestamp,
            cur_timestamp: cur_timestamp,
            prodt_name: self.prodt_name().to_string(),
            prodt_money: *self.prodt_money(),
        };

        Ok(consume_prodt_detail)
    }
}
