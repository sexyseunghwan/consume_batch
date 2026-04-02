use std::collections::HashSet;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::common::*;
use crate::models::holiday::{HolidayApiResponse, HolidayBodyContent};
use crate::service_trait::public_data_service::PublicDataService;

// ============================================================================
// PublicDataServiceImpl
// ============================================================================

pub struct PublicDataServiceImpl {
    api_key: String,
    client: reqwest::Client,
}

impl PublicDataServiceImpl {
    /// Creates a new public-data client with the provided API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl PublicDataService for PublicDataServiceImpl {
    /// Fetches Korean public holiday dates for the inclusive year range.
    async fn fetch_korea_holiday_set(
        &self,
        start_year: i32,
        end_year: i32,
    ) -> anyhow::Result<HashSet<NaiveDate>> {
        let mut holidays: HashSet<NaiveDate> = HashSet::new();
        let encoded_key: String = utf8_percent_encode(&self.api_key, NON_ALPHANUMERIC).to_string();

        for year in start_year..=end_year {
            for month in 1u32..=12 {
                let url: String = format!(
                    "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getHoliDeInfo\
                     ?serviceKey={}&solYear={}&solMonth={:02}&numOfRows=100",
                    encoded_key, year, month
                );
                let xml: String = self
                    .client
                    .get(&url)
                    .send()
                    .await
                    .with_context(|| format!("Holiday API request failed for {year}-{month:02}"))?
                    .text()
                    .await
                    .with_context(|| {
                        format!("Holiday API read body failed for {year}-{month:02}")
                    })?;

                let parsed: HolidayApiResponse =
                    quick_xml::de::from_str(&xml).with_context(|| {
                        format!(
                            "Holiday API XML parse failed for {year}-{month:02}: body={}",
                            &xml[..xml.len().min(300)]
                        )
                    })?;

                if parsed.header.result_code != "00" {
                    error!(
                        "[PublicDataServiceImpl::fetch_korea_holiday_set] API error for {year}-{month:02}: {} {}",
                        parsed.header.result_code, parsed.header.result_msg
                    );
                    continue;
                }

                let body: HolidayBodyContent = parsed.body;
                if body.total_count == 0 {
                    continue;
                }

                let items = match body.items {
                    Some(i) => i.items,
                    None => continue,
                };

                for item in items {
                    if item.is_holiday != "Y" {
                        continue;
                    }
                    let locdate = item.locdate;
                    let y = (locdate / 10000) as i32;
                    let m = (locdate % 10000) / 100;
                    let d = locdate % 100;
                    if let Some(date) = NaiveDate::from_ymd_opt(y, m, d) {
                        holidays.insert(date);
                    }
                }
            }

            info!(
                "[PublicDataServiceImpl::fetch_korea_holiday_set] Fetched holidays for year {}",
                year
            );
        }

        Ok(holidays)
    }
}
