use reqwest::Client;
use rust_decimal::Decimal;

use crate::app_config::AppConfig;
use crate::common::*;
use crate::entity::currency_exchange_rate_snapshot;

static HTTP_CLIENT: once_lazy<Client> = once_lazy::new(reqwest::Client::new);

/// Twelve Data `/exchange_rate` success payload.
#[derive(Debug, Deserialize)]
struct TwelveDataRatePayload {
    rate: f64,
}

/// Twelve Data `/price` success payload.
#[derive(Debug, Deserialize)]
struct TwelveDataPricePayload {
    price: String,
}

/// Twelve Data error payload.
#[derive(Debug, Deserialize)]
struct TwelveDataErrorPayload {
    code: u32,
    message: String,
}

/// Untagged union for `/exchange_rate` response shapes.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TwelveDataApiResponse {
    Rate(TwelveDataRatePayload),
    Error(TwelveDataErrorPayload),
}

/// Untagged union for `/price` response shapes.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TwelveDataPriceResponse {
    Price(TwelveDataPricePayload),
    Error(TwelveDataErrorPayload),
}

pub async fn fetch_exchange_rate(base: &str, target: &str) -> anyhow::Result<f64> {
    let app_config: &AppConfig = AppConfig::get_global()?;

    let url: String = format!(
        "{}/exchange_rate?symbol={}/{}&apikey={}",
        app_config.twelve_data_api(),
        base,
        target,
        app_config.twelve_data_api_key()
    );

    let response: TwelveDataApiResponse = HTTP_CLIENT
        .get(&url)
        .send()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_exchange_rate] HTTP request failed for {}/{}: {:#}",
                base, target, e
            );
        })?
        .json::<TwelveDataApiResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_exchange_rate] Failed to parse response for {}/{}: {:#}",
                base, target, e
            );
        })?;

    match response {
        TwelveDataApiResponse::Rate(payload) => Ok(payload.rate),
        TwelveDataApiResponse::Error(err) => Err(anyhow!(
            "Twelve Data API error (code={}) for {}/{}: {}",
            err.code,
            base,
            target,
            err.message,
        )),
    }
}

pub async fn fetch_stock_price(symbol: &str) -> anyhow::Result<Decimal> {
    let app_config: &AppConfig = AppConfig::get_global()?;

    let url: String = format!(
        "{}/price?symbol={}&apikey={}",
        app_config.twelve_data_api(),
        symbol,
        app_config.twelve_data_api_key()
    );

    let response: TwelveDataPriceResponse = HTTP_CLIENT
        .get(&url)
        .send()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_stock_price] HTTP request failed for {}: {:#}",
                symbol, e
            );
        })?
        .json::<TwelveDataPriceResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_stock_price] Failed to parse response for {}: {:#}",
                symbol, e
            );
        })?;

    match response {
        TwelveDataPriceResponse::Price(payload) => {
            payload.price.trim().parse::<Decimal>().map_err(|e| {
                anyhow!(
                    "[fetch_stock_price] Failed to parse price '{}' for {}: {}",
                    payload.price,
                    symbol,
                    e
                )
            })
        }
        TwelveDataPriceResponse::Error(err) => Err(anyhow!(
            "Twelve Data API error (code={}) for {}: {}",
            err.code,
            symbol,
            err.message,
        )),
    }
}
