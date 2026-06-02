use rust_decimal::Decimal;

use crate::common::*;

#[derive(Debug, Serialize, Getters, new)]
#[getset(get = "pub")]
pub struct CurrentStockPriceDto {
    pub stock_code: String,
    pub current_price: Decimal,
    pub prev_close: Decimal,
    pub change_rate: f64,
    pub high_price: Decimal,
    pub low_price: Decimal,
    pub volume: u64,
}

#[derive(Debug, Serialize, Getters, new)]
#[getset(get = "pub")]
pub struct CurrentOverseasStockPriceDto {
    pub exchange_code: String,
    pub stock_code: String,
    pub current_price: Decimal,
    pub prev_close: Decimal,
    pub change_amount: Decimal,
    pub change_rate: f64,
    pub volume: u64,
}
