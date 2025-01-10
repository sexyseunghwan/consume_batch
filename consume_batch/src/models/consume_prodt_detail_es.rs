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
    pub prodt_type: Option<String>,
}

//ConsumeProdtDetail

impl ConsumeProdtDetailES {
    #[doc = "Functions that convert elasticsearch schema to schema data formats in MySQL format"]
    pub fn transfer_to_consume_prodt_detail(&self) -> Result<ConsumeProdtDetail, anyhow::Error> {
        let formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%Z",
            "%Y-%m-%dT%H:%M:%S%.fZ",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M",
        ];

        let mut timestamp =
            NaiveDateTime::parse_from_str("1970-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")?;

        for format in formats {
            if let Ok(parsed) = NaiveDateTime::parse_from_str(self.timestamp(), format) {
                timestamp = parsed;
                break;
            }
        }

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
            reg_dt: None,
            chg_dt: None,
            reg_id: None,
            chg_id: None,
        };

        Ok(consume_prodt_detail)
    }
}
