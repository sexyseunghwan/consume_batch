use crate::common::*;

#[derive(Debug, Deserialize)]
pub struct KisTokenResponse {
    pub access_token: String,
    pub access_token_token_expired: String,
    #[allow(dead_code)]
    pub token_type: String,
    #[allow(dead_code)]
    pub expires_in: u64,
}

#[derive(Debug, Deserialize)]
pub struct KisPriceOutput {
    pub stck_prpr: String, // 주식 현재가
    pub stck_sdpr: String, // 전일 종가
    pub prdy_ctrt: String, // 전일 대비율 (%)
    pub stck_hgpr: String, // 당일 최고가
    pub stck_lwpr: String, // 당일 최저가
    pub acml_vol: String,  // 누적 거래량
}

#[derive(Debug, Deserialize)]
pub struct KisPriceResponse {
    pub rt_cd: String, // "0" = 정상
    pub msg_cd: String,
    pub msg1: String,
    pub output: KisPriceOutput,
}

#[derive(Debug, Deserialize)]
pub struct KisOverseasPriceOutput {
    pub last: String, // 현재가
    pub base: String, // 전일 종가
    pub diff: String, // 전일 대비
    pub rate: String, // 등락율 (%)
    pub tvol: String, // 당일 거래량
}

#[derive(Debug, Deserialize)]
pub struct KisOverseasPriceResponse {
    pub rt_cd: String, // "0" = 정상
    pub msg_cd: String,
    pub msg1: String,
    pub output: KisOverseasPriceOutput,
}
