//! Asset price sync batch jobs.

use rust_decimal::Decimal;
use sea_orm::sea_query::extension::mysql;

use crate::models::{
    Crypto, CurrencyExchangeRateSnapshot, Stock, StockAssetAmount, StockType,
    UserCurrentAssetSnapshot, batch_schedule::*,
};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

use crate::api::twelve_data_api::*;

use super::BatchServiceImpl;

/// Shared batch-loop logic for asset price sync jobs.
///
/// Iterates over the asset table in `batch_size` steps, calls
/// [`fetch_stock_price`] for each `api_symbol`, and bulk-updates the results.
/// Individual API failures are logged and skipped; DB errors abort the function.
///
/// # Arguments
///
/// * `batch_size`  - Rows per iteration
/// * `label`       - Job name used in log messages (e.g. `"sync_stock_price"`)
/// * `fetch_fn`    - Returns the next page as `(seq, api_symbol)` pairs
/// * `update_fn`   - Persists the collected `price_map` to the DB
async fn sync_asset_price<F, G>(
    batch_size: u64,
    label: &str,
    fetch_fn: F,
    update_fn: G,
) -> anyhow::Result<()>
where
    F: AsyncFn(u64, u64) -> anyhow::Result<Vec<(i64, String)>>,
    G: AsyncFn(HashMap<i64, Decimal>) -> anyhow::Result<()>,
{
    let mut offset: u64 = 0;
    let mut total_count: usize = 0;
    let mut success_count: usize = 0;
    let mut fail_count: usize = 0;

    info!(
        "[BatchServiceImpl::{}] Starting price sync (batch_size={}).",
        label, batch_size
    );

    loop {
        let items: Vec<(i64, String)> = fetch_fn(offset, batch_size).await.inspect_err(|e| {
            error!(
                "[BatchServiceImpl::{}] Failed to fetch batch (offset={}): {:#}",
                label, offset, e
            );
        })?;

        if items.is_empty() {
            break;
        }

        total_count += items.len();
        let mut price_map: HashMap<i64, Decimal> = HashMap::new();

        for (seq, symbol) in &items {
            match fetch_stock_price(symbol).await {
                Ok(price) => {
                    price_map.insert(*seq, price);
                }
                Err(e) => {
                    error!(
                        "[BatchServiceImpl::{}] Failed to fetch price for {} (seq={}): {:#}",
                        label, symbol, seq, e
                    );
                    fail_count += 1;
                }
            }
        }

        if !price_map.is_empty() {
            let batch_success: usize = price_map.len();
            update_fn(price_map).await.inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] Bulk update failed (offset={}): {:#}",
                    label, offset, e
                );
            })?;
            success_count += batch_success;
        }

        offset += batch_size;
    }

    info!(
        "[BatchServiceImpl::{}] Completed: total={}, success={}, failed={}.",
        label, total_count, success_count, fail_count
    );

    Ok(())
}

impl<M, E, C, P, D, I, S> BatchServiceImpl<M, E, C, P, D, I, S>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
{
    pub(super) async fn sync_currency_exchange_rates(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        batch_log!(
            info,
            "[BatchServiceImpl::sync_currency_exchange_rates] Starting currency price sync."
        );

        let currency_snapshots: Vec<CurrencyExchangeRateSnapshot> = mysql_service
            .find_currency_exchange_rate_snapshot()
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_currency_exchange_rates] Error at `target_currency_infos`: {:#}",
                    e
                );
            })?;

        if currency_snapshots.is_empty() {
            info!(
                "[BatchServiceImpl::sync_currency_exchange_rates] No snapshot rows found, skipping."
            );
            return Ok(());
        }

        let mut snapshot_map: HashMap<i64, f64> = HashMap::new();

        for snapshot in currency_snapshots {
            let base: &str = snapshot.base_currency_code();
            let target: &str = snapshot.target_currency_code();
            let seq: i64 = snapshot.exchange_rate_snapshot_seq;

            let exchange_rate: f64 = match fetch_exchange_rate(base, target).await {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        "[BatchServiceImpl::sync_currency_exchange_rates] Error at `exchange_rate`: {:#}",
                        e
                    );
                    continue;
                }
            };

            snapshot_map.insert(seq, exchange_rate);
        }

        if snapshot_map.is_empty() {
            warn!(
                "[BatchServiceImpl::sync_currency_exchange_rates] All API fetches failed, skipping DB update."
            );
            return Ok(());
        }

        mysql_service
            .modify_currency_exchange_rate_snapshot_bulk(&snapshot_map)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_currency_exchange_rates] Bulk update failed: {:#}",
                    e
                );
            })?;

        batch_log!(
            info,
            "[BatchServiceImpl::sync_currency_exchange_rates] Updated {} exchange rate(s).",
            snapshot_map.len()
        );

        Ok(())
    }

    pub(super) async fn sync_stock_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let ms1: Arc<M> = Arc::clone(mysql_service);
        let ms2: Arc<M> = Arc::clone(mysql_service);

        sync_asset_price(
            batch_size,
            "sync_stock_price",
            async move |offset, limit| {
                ms1.find_stock_batch(offset, limit).await.map(|v| {
                    v.into_iter()
                        .map(|s| (*s.stock_seq(), s.api_symbol().clone()))
                        .collect()
                })
            },
            async move |price_map| ms2.modify_stock_price_bulk(&price_map).await,
        )
        .await
    }

    pub(super) async fn sync_crypto_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let ms1: Arc<M> = Arc::clone(mysql_service);
        let ms2: Arc<M> = Arc::clone(mysql_service);

        sync_asset_price(
            batch_size,
            "sync_crypto_price",
            async move |offset, limit| {
                ms1.find_crypto_batch(offset, limit).await.map(|v| {
                    v.into_iter()
                        .map(|c| (*c.crypto_seq(), c.api_symbol().clone()))
                        .collect()
                })
            },
            async move |price_map| ms2.modify_crypto_price_bulk(&price_map).await,
        )
        .await
    }

    pub(super) async fn sync_current_asset_total(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let stock_types: Vec<StockType> = mysql_service.find_stock_types().await?;

        let batch_size: u64 = *schedule_item.batch_size() as u64;

        for s_type in stock_types {
            let currency: &str = s_type.currency_code();
            let mut offset: u64 = 0;
            //let snapshots: HashMap<i64, UserCurrentAssetSnapshot> = HashMap::new();

            loop {
                /*
                    Fetch all users in pages.
                    Since each aggregation step uses these user_seqs as an IN filter,
                    users who do not own a spcific asset type are still included in the process.
                */
                let user_seqs: Vec<i64> = mysql_service
                    .find_user_seq_batch(offset, batch_size)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_current_asset_total] \
                             find_user_seq_batch failed (offset={}): {:#}",
                            offset, e
                        );
                    })?;

                if user_seqs.is_empty() {
                    break;
                }

                // 1. Get stock asset
                let stock_assets: Vec<StockAssetAmount> = mysql_service
                    .find_stock_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_current_asset_total] \
                             find_stock_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })?;

                // let stock_amount_map: HashMap<i64, Decimal> = stock_assets
                //     .iter()
                //     .filter_map(|sa| sa.stock_sum().map(|sum| (*sa.user_seq(), sum)))
                //     .collect();

                // 2. Get crypto asset

                // 3. Get cash asset

                // 4. Get deposit asset

                // 5. Get saving asset

                offset += batch_size;
            }
        }

        Ok(())
    }
}
