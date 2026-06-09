use chrono::{FixedOffset, NaiveDateTime, TimeZone};
use reqwest::Client;
use rust_decimal::Decimal;

use crate::api::dto::kis_dto::{CurrentOverseasStockPriceDto, CurrentStockPriceDto};
use crate::api::model::kis_model::{KisOverseasPriceResponse, KisPriceResponse, KisTokenResponse};
use crate::app_config::AppConfig;
use crate::common::*;
use crate::models::KisApiToken;
use crate::service_trait::mysql_service::MysqlService;
use crate::service_trait::redis_service::RedisService;

static HTTP_CLIENT: once_lazy<Client> = once_lazy::new(reqwest::Client::new);

/// Fetch OAuth2 access token from KIS (`/oauth2/tokenP`).
pub async fn fetch_kis_access_token() -> anyhow::Result<KisTokenResponse> {
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
        .inspect_err(|e| {
            error!(
                "[kis_api::fetch_kis_access_token] HTTP request failed: {:#}",
                e
            )
        })?
        .json::<KisTokenResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[kis_api::fetch_kis_access_token] Failed to parse token response: {:#}",
                e
            )
        })?;

    Ok(resp)
}

fn parse_token_expired_at(s: &str) -> anyhow::Result<DateTime<Utc>> {
    let ndt: NaiveDateTime =
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|e| {
            anyhow!(
                "[kis_api::parse_token_expired_at] Failed to parse '{}': {}",
                s,
                e
            )
        })?;
    let kst: FixedOffset = FixedOffset::east_opt(9 * 3600)
        .ok_or_else(|| anyhow!("[kis_api::parse_token_expired_at] Invalid KST offset"))?;
    let kst_dt: DateTime<FixedOffset> = kst
        .from_local_datetime(&ndt)
        .single()
        .ok_or_else(|| anyhow!("[kis_api::parse_token_expired_at] Ambiguous or invalid KST datetime: {}", s))?;
    Ok(kst_dt.with_timezone(&Utc))
}

/// Returns a valid KIS access token, refreshing from the API when necessary.
///
/// Resolution order:
/// 1. Redis cache (key = `AppConfig::redis_kis_access_token`)
/// 2. `KIS_API_TOKEN` DB row that has not yet expired
/// 3. New OAuth2 grant via `fetch_kis_access_token()` — result is saved to both DB and Redis
///
/// Redis read/write failures are logged and silently skipped so that DB and API
/// paths always continue.
pub async fn find_or_refresh_kis_access_token<R, M>(
    redis_service: &Arc<R>,
    mysql_service: &Arc<M>,
) -> anyhow::Result<String>
where
    R: RedisService,
    M: MysqlService,
{
    let cfg: &AppConfig = AppConfig::get_global()?;
    let redis_key: &str = cfg.redis_kis_access_token();
    let now: DateTime<Utc> = Utc::now();

    // 1. Redis cache hit
    match redis_service.find_string(redis_key).await {
        Ok(Some(token)) => {
            info!("[kis_api::find_or_refresh_kis_access_token] Redis cache hit.");
            return Ok(token);
        }
        Ok(None) => {}
        Err(e) => {
            warn!(
                "[kis_api::find_or_refresh_kis_access_token] Redis read failed, continuing: {:#}",
                e
            );
        }
    }

    // 2. DB lookup
    let db_token: Option<KisApiToken> = match mysql_service.find_kis_api_token().await {
        Ok(opt) => opt,
        Err(e) => {
            warn!(
                "[kis_api::find_or_refresh_kis_access_token] DB read failed, will fetch new token: {:#}",
                e
            );
            None
        }
    };

    // 3. Valid DB token — cache it in Redis and return
    if let Some(row) = db_token {
        if *row.token_expired_at() > now {
            let ttl: u64 = (*row.token_expired_at() - now).num_seconds().max(1) as u64;
            if let Err(e) = redis_service
                .input_string(redis_key, row.access_token(), Some(ttl))
                .await
            {
                warn!(
                    "[kis_api::find_or_refresh_kis_access_token] Redis write failed: {:#}",
                    e
                );
            }
            info!(
                "[kis_api::find_or_refresh_kis_access_token] Token from DB (valid, TTL={}s).",
                ttl
            );
            return Ok(row.access_token().clone());
        }
        info!("[kis_api::find_or_refresh_kis_access_token] DB token expired, fetching new one.");
    } else {
        info!("[kis_api::find_or_refresh_kis_access_token] No DB token, fetching new one.");
    }

    // 4. Fetch new token from KIS API
    let resp: KisTokenResponse = fetch_kis_access_token().await?;
    let expired_at: DateTime<Utc> = parse_token_expired_at(&resp.access_token_token_expired)?;
    let ttl: u64 = (expired_at - now).num_seconds().max(1) as u64;

    // 5. Persist to DB
    if let Err(e) = mysql_service
        .modify_kis_api_token(resp.access_token.clone(), expired_at)
        .await
    {
        error!(
            "[kis_api::find_or_refresh_kis_access_token] DB update failed: {:#}",
            e
        );
    }

    // 6. Cache in Redis
    if let Err(e) = redis_service
        .input_string(redis_key, &resp.access_token, Some(ttl))
        .await
    {
        error!(
            "[kis_api::find_or_refresh_kis_access_token] Redis write failed: {:#}",
            e
        );
    }

    info!(
        "[kis_api::find_or_refresh_kis_access_token] New token fetched and stored (TTL={}s).",
        ttl
    );
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
pub async fn fetch_current_stock_price<R, M>(
    stock_code: &str,
    redis_service: &Arc<R>,
    mysql_service: &Arc<M>,
) -> anyhow::Result<CurrentStockPriceDto>
where
    R: RedisService,
    M: MysqlService,
{
    let cfg: &AppConfig = AppConfig::get_global()?;

    let app_key: &str = cfg.kis_app_key();
    let app_secret: &str = cfg.kis_app_secret();
    let base_url: &str = cfg.kis_api_base_url();

    let access_token: String = find_or_refresh_kis_access_token(redis_service, mysql_service)
        .await
        .inspect_err(|e| {
            error!("[kis_api::fetch_current_stock_price] {:#}", e);
        })?;

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
                "[kis_api::fetch_current_stock_price] HTTP failed for {}: {:#}",
                stock_code, e
            )
        })?
        .json::<KisPriceResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[kis_api::fetch_current_stock_price] JSON parse failed for {}: {:#}",
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
                    "[kis_api::fetch_current_stock_price] Failed to parse {} '{}' for {}: {:#}",
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
                    "[kis_api::fetch_current_stock_price] Failed to parse prdy_ctrt '{}' for {}: {:#}",
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
                    "[kis_api::fetch_current_stock_price] Failed to parse acml_vol '{}' for {}: {:#}",
                    o.acml_vol, stock_code, e
                )
            })
            .map_err(anyhow::Error::from)?,
    ))
}

/// Fetch current overseas stock price from KIS.
///
/// `exchange_code` — 거래소 코드 (e.g. `"NAS"` NASDAQ, `"NYS"` NYSE, `"AMS"` AMEX)
/// `symbol`        — 종목코드 (e.g. `"AAPL"`, `"TSLA"`)
pub async fn fetch_current_overseas_stock_price<R, M>(
    exchange_code: &str,
    symbol: &str,
    redis_service: &Arc<R>,
    mysql_service: &Arc<M>,
) -> anyhow::Result<CurrentOverseasStockPriceDto>
where
    R: RedisService,
    M: MysqlService,
{
    let cfg: &AppConfig = AppConfig::get_global()?;
    let app_key: &str = cfg.kis_app_key();
    let app_secret: &str = cfg.kis_app_secret();
    let base_url: &str = cfg.kis_api_base_url();

    let access_token: String =
        find_or_refresh_kis_access_token(redis_service, mysql_service).await?;

    let url: String = format!(
        "{}/uapi/overseas-price/v1/quotations/price?AUTH=&EXCD={}&SYMB={}",
        base_url, exchange_code, symbol
    );

    let resp: KisOverseasPriceResponse = HTTP_CLIENT
        .get(&url)
        .header("content-type", "application/json; charset=utf-8")
        .header("authorization", format!("Bearer {}", access_token))
        .header("appkey", app_key)
        .header("appsecret", app_secret)
        .header("tr_id", "HHDFS00000300")
        .send()
        .await
        .inspect_err(|e| {
            error!(
                "[kis_api::fetch_current_overseas_stock_price] HTTP failed for {}/{}: {:#}",
                exchange_code, symbol, e
            )
        })?
        .json::<KisOverseasPriceResponse>()
        .await
        .inspect_err(|e| {
            error!(
                "[kis_api::fetch_current_overseas_stock_price] JSON parse failed for {}/{}: {:#}",
                exchange_code, symbol, e
            )
        })?;

    if resp.rt_cd != "0" {
        return Err(anyhow!(
            "[kis_api::fetch_current_overseas_stock_price] KIS API error for {}/{}: [{}] {}",
            exchange_code,
            symbol,
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
                    "[kis_api::fetch_current_overseas_stock_price] Failed to parse {} '{}' for {}/{}: {:#}",
                    field, raw, exchange_code, symbol, e
                )
            })
            .map_err(anyhow::Error::from)
    };

    Ok(CurrentOverseasStockPriceDto::new(
        exchange_code.to_string(),
        symbol.to_string(),
        parse_decimal(&o.last, "last")?,
        parse_decimal(&o.base, "base")?,
        parse_decimal(&o.diff, "diff")?,
        o.rate
            .trim()
            .trim_start_matches('+')
            .parse::<f64>()
            .inspect_err(|e| {
                error!(
                    "[kis_api::fetch_current_overseas_stock_price] Failed to parse rate '{}' for {}/{}: {:#}",
                    o.rate, exchange_code, symbol, e
                )
            })
            .map_err(anyhow::Error::from)?,
        o.tvol
            .trim()
            .parse::<u64>()
            .inspect_err(|e| {
                error!(
                    "[kis_api::fetch_current_overseas_stock_price] Failed to parse tvol '{}' for {}/{}: {:#}",
                    o.tvol, exchange_code, symbol, e
                )
            })
            .map_err(anyhow::Error::from)?,
    ))
}
