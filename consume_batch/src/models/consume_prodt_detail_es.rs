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

// #[doc = "이거가 필요없을 수도?"]
// pub fn transfer_to_consume_prodt_detail_vector(consume_prodt_details: &Vec<ConsumeProdtDetail>) -> Result<Vec<ConsumeProdtDetailES>, anyhow::Error> {

//     let mut consume_prodt_details_es: Vec<ConsumeProdtDetailES> = Vec::new();

//     for prodt_detail in consume_prodt_details {
//         let prodt_details_es = ConsumeProdtDetailES::new(
//             prodt_detail.timestamp.to_string(),
//             Some(prodt_detail.cur_timestamp.to_string()),
//             prodt_detail.prodt_name.to_string(),
//             prodt_detail.prodt_money,
//             Some(String::from("test"))); // 여기 추후에 바꿔야하는데...

//         consume_prodt_details_es.push(prodt_details_es);
//     }

//     Ok(consume_prodt_details_es)
// }

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

        let mut timestamp: NaiveDateTime =
            NaiveDateTime::parse_from_str("1970-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S")?;

        for format in formats {
            if let Ok(parsed) = NaiveDateTime::parse_from_str(self.timestamp(), format) {
                timestamp = parsed;
                break;
            }
        }

        let cur_timestamp: NaiveDateTime;

        if let Some(value) = self.cur_timestamp() {
            cur_timestamp = get_naive_datetime_from_str(value, "%Y-%m-%dT%H:%M:%SZ")?;
        } else {
            cur_timestamp = timestamp.clone();
        }

        let consume_prodt_detail: ConsumeProdtDetail = ConsumeProdtDetail {
            timestamp: timestamp,
            cur_timestamp: cur_timestamp,
            prodt_name: self.prodt_name().to_string(),
            prodt_money: *self.prodt_money(),
            reg_dt: Some(cur_timestamp),
            chg_dt: None,
            reg_id: Some(String::from("system")),
            chg_id: None,
        };
        
        Ok(consume_prodt_detail)
    }
}
