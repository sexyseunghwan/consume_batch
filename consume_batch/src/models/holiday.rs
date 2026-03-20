use crate::common::*;

/// 공공데이터포털 특일 정보 API XML 응답 루트.
///
/// ```xml
/// <response>
///   <header>...</header>
///   <body>...</body>
/// </response>
/// ```
#[derive(Deserialize)]
pub struct HolidayApiResponse {
    pub header: HolidayHeader,
    pub body: HolidayBodyContent,
}

#[derive(Deserialize)]
pub struct HolidayHeader {
    #[serde(rename = "resultCode")]
    pub result_code: String,
    #[serde(rename = "resultMsg")]
    pub result_msg: String,
}

#[derive(Deserialize)]
pub struct HolidayBodyContent {
    pub items: Option<HolidayItems>,
    #[serde(rename = "totalCount")]
    pub total_count: i64,
}

#[derive(Deserialize)]
pub struct HolidayItems {
    /// quick-xml은 동일 이름의 반복 태그를 Vec으로 자동 수집한다.
    #[serde(rename = "item", default)]
    pub items: Vec<HolidayItem>,
}

#[derive(Deserialize)]
pub struct HolidayItem {
    /// 날짜 (YYYYMMDD 정수, 예: 20260216)
    pub locdate: u32,
    /// 법정 공휴일 여부 ("Y" / "N")
    #[serde(rename = "isHoliday")]
    pub is_holiday: String,
}
