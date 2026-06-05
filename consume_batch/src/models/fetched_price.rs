use rust_decimal::Decimal;

pub struct FetchedPrice {
    pub seq: i64,
    pub symbol: String,
    pub currency_code: String,
    pub name: String,
    pub price: Decimal,
}
