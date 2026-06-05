use reqwest::Client;

use crate::app_config::AppConfig;
use crate::common::*;

static HTTP_CLIENT: once_lazy<Client> = once_lazy::new(reqwest::Client::new);

/// Twelve Data `/exchange_rate` success payload.
#[derive(Debug, Deserialize)]
struct TwelveDataRatePayload {
    rate: f64,
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
