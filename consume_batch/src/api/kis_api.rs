use reqwest::Client;
use rust_decimal::Decimal;

use crate::api::dto::kis_dto::CurrentStockPriceDto;
use crate::api::model::kis_model::{KisPriceResponse, KisTokenResponse};
use crate::app_config::AppConfig;
use crate::common::*;

static HTTP_CLIENT: once_lazy<Client> = once_lazy::new(reqwest::Client::new);

/// Fetch OAuth2 access token from KIS (`/oauth2/tokenP`).
pub async fn fetch_kis_access_token() -> anyhow::Result<String> {
    let cfg: &AppConfig = AppConfig::get_global()?;

    let app_key: &str = cfg.kis_app_key();
    let app_secret: &str = cfg.kis_app_secret();
    let base_url: &str = cfg.kis_api_base_url();

    let url: String = format!("{}/oauth2/tokenP", base_url);

    let body: Value = json!({
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret,
    });

    let resp: KisTokenResponse = HTTP_CLIENT
        .post(&url)
        .header("content-type", "application/json; charset=utf-8")
        .json(&body)
        .send()
        .await
        .inspect_err(|e| error!("[fetch_kis_access_token] HTTP request failed: {:#}", e))?
        .json::<KisTokenResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_kis_access_token] Failed to parse token response: {:#}",
                e
            )
        })?;

    Ok(resp.access_token)
}

/// Fetch current domestic stock price from KIS.
///
/// `stock_code` — 종목코드 (e.g. `"000660"` for SK Hynix)
///
/// # Example
/// ```no_run
/// let dto = fetch_current_stock_price("000660").await?;
/// println!("SK하이닉스 현재가: {}", dto.current_price());
/// ```
pub async fn fetch_current_stock_price(stock_code: &str) -> anyhow::Result<CurrentStockPriceDto> {
    let cfg: &AppConfig = AppConfig::get_global()?;

    let app_key: &str = cfg.kis_app_key();
    let app_secret: &str = cfg.kis_app_secret();
    let base_url: &str = cfg.kis_api_base_url();

    let access_token: String = fetch_kis_access_token().await?;

    let url: String = format!(
        "{}/uapi/domestic-stock/v1/quotations/inquire-price\
         ?FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD={}",
        base_url, stock_code
    );

    let resp: KisPriceResponse = HTTP_CLIENT
        .get(&url)
        .header("content-type", "application/json; charset=utf-8")
        .header("authorization", format!("Bearer {}", access_token))
        .header("appkey", app_key)
        .header("appsecret", app_secret)
        .header("tr_id", "FHKST01010100")
        .send()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_current_stock_price] HTTP failed for {}: {:#}",
                stock_code, e
            )
        })?
        .json::<KisPriceResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[fetch_current_stock_price] JSON parse failed for {}: {:#}",
                stock_code, e
            )
        })?;

    if resp.rt_cd != "0" {
        return Err(anyhow!(
            "[kis_api::fetch_current_stock_price] KIS API error for {}: [{}] {}",
            stock_code,
            resp.msg_cd,
            resp.msg1
        ));
    }

    let o = resp.output;

    let parse_decimal = |raw: &str, field: &str| -> anyhow::Result<Decimal> {
        raw.trim()
            .parse::<Decimal>()
            .inspect_err(|e| {
                error!(
                    "[fetch_current_stock_price] Failed to parse {} '{}' for {}: {:#}",
                    field, raw, stock_code, e
                )
            })
            .map_err(anyhow::Error::from)
    };

    Ok(CurrentStockPriceDto::new(
        stock_code.to_string(),
        parse_decimal(&o.stck_prpr, "stck_prpr")?,
        parse_decimal(&o.stck_sdpr, "stck_sdpr")?,
        o.prdy_ctrt
            .trim()
            .parse::<f64>()
            .inspect_err(|e| {
                error!(
                    "[fetch_current_stock_price] Failed to parse prdy_ctrt '{}' for {}: {:#}",
                    o.prdy_ctrt, stock_code, e
                )
            })
            .map_err(anyhow::Error::from)?,
        parse_decimal(&o.stck_hgpr, "stck_hgpr")?,
        parse_decimal(&o.stck_lwpr, "stck_lwpr")?,
        o.acml_vol
            .trim()
            .parse::<u64>()
            .inspect_err(|e| {
                error!(
                    "[fetch_current_stock_price] Failed to parse acml_vol '{}' for {}: {:#}",
                    o.acml_vol, stock_code, e
                )
            })
            .map_err(anyhow::Error::from)?,
    ))
}
