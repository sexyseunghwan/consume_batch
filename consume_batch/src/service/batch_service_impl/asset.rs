//! Asset price sync batch jobs.

use rust_decimal::Decimal;

//use crate::api::kis_api::fetch_current_stock_price;
use crate::entity::{user_asset_snapshot_summary, user_current_asset_snapshot};
use crate::dtos::{AssetAmount, AssetTypeAmounts, PriceFetchItem};
use crate::models::{
    CurrencyExchangeRateSnapshot, Market, UserAssetSnapshotSummary, UserCurrentAssetSnapshot,
    batch_schedule::*, CurrencyCode
};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    redis_service::RedisService, smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

use crate::api::{kis_api, twelve_data_api};

use super::BatchServiceImpl;

// Converts asset amount rows into a user-seq keyed lookup map.
fn to_amount_map(amounts: Vec<AssetAmount>) -> HashMap<i64, Decimal> {
    amounts
        .into_iter()
        .filter_map(|row| row.asset_sum.map(|sum| (row.user_seq, sum)))
        .collect()
}

// Synchronizes prices for assets that can be fetched by API symbol.
async fn sync_asset_price<F, G, P>(
    batch_size: u64,
    label: &str,
    fetch_fn: F,
    update_fn: G,
    price_fn: P,
) -> anyhow::Result<()>
where
    F: AsyncFn(u64, u64) -> anyhow::Result<Vec<PriceFetchItem>>,
    G: AsyncFn(HashMap<i64, Decimal>) -> anyhow::Result<()>,
    P: AsyncFn(&PriceFetchItem) -> anyhow::Result<Decimal>,
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
        let items: Vec<PriceFetchItem> = fetch_fn(offset, batch_size).await.inspect_err(|e| {
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

        /* 이 부분이 중요함 API CALL 을 한번에 너무 많이 하면 DENY 되기 때문에 sleep을 걸어줘야 한다. */
        for item in &items {
            match price_fn(item).await {
                Ok(price) => {
                    price_map.insert(item.seq, price);
                }
                Err(e) => {
                    error!(
                        "[BatchServiceImpl::{}] Failed to fetch price for {} (seq={}): {:#}",
                        label, item.symbol, item.seq, e
                    );
                    fail_count += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
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

impl<M, E, C, P, D, I, S, R> BatchServiceImpl<M, E, C, P, D, I, S, R>
where
    M: MysqlService + Send + Sync + 'static,
    E: ElasticService + Send + Sync + 'static,
    C: ConsumeService + Send + Sync + 'static,
    P: ProducerService + Send + Sync + 'static,
    D: PublicDataService + Send + Sync + 'static,
    I: IndexingService + Send + Sync + 'static,
    S: SmtpService + Send + Sync + 'static,
    R: RedisService + Send + Sync + 'static,
{
    // Fetches active exchange-rate snapshots from MySQL and refreshes them from the external API.
    pub(super) async fn sync_currency_exchange_rates(mysql_service: &Arc<M>) -> anyhow::Result<()> {
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

            let exchange_rate: f64 = match twelve_data_api::fetch_exchange_rate(base, target).await
            {
                Ok(rate) => rate,
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

    // Synchronizes stock prices in paged batches.
    pub(super) async fn sync_stock_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        redis_service: &Arc<R>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let mysql_for_fetch: Arc<M> = Arc::clone(mysql_service);
        let mysql_for_update: Arc<M> = Arc::clone(mysql_service);
        let mysql_for_price: Arc<M> = Arc::clone(mysql_service);
        let redis_for_price: Arc<R> = Arc::clone(redis_service);

        sync_asset_price(
            batch_size,
            "sync_stock_price",
            async move |offset, limit| {
                mysql_for_fetch
                    .find_stock_batch(offset, limit)
                    .await
                    .map(|v| {
                        v.into_iter()
                            .map(|s| PriceFetchItem {
                                seq: *s.stock_seq(),
                                symbol: s.api_symbol().clone(),
                                currency_code: s.currency_code().clone(),
                                market_alias: s.market_alias().clone(),
                            })
                            .collect()
                    })
            },
            async move |price_map| mysql_for_update.modify_stock_price_bulk(&price_map).await,
            async move |item: &PriceFetchItem| {
                if item.currency_code == "USD" {
                    kis_api::fetch_current_overseas_stock_price(
                        &item.market_alias,
                        &item.symbol,
                        &redis_for_price,
                        &mysql_for_price,
                    )
                    .await
                    .map(|dto| *dto.current_price())
                } else {
                    kis_api::fetch_current_stock_price(
                        &item.symbol,
                        &redis_for_price,
                        &mysql_for_price,
                    )
                    .await
                    .map(|dto| *dto.current_price())
                }
            },
        )
        .await?;

        Ok(())
    }

    // Synchronizes crypto prices in paged batches.
    pub(super) async fn sync_crypto_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let mysql_for_fetch: Arc<M> = Arc::clone(mysql_service);
        let mysql_for_update: Arc<M> = Arc::clone(mysql_service);

        sync_asset_price(
            batch_size,
            "sync_crypto_price",
            async move |offset, limit| {
                mysql_for_fetch
                    .find_crypto_batch(offset, limit)
                    .await
                    .map(|v| {
                        v.into_iter()
                            .map(|c| PriceFetchItem {
                                seq: *c.crypto_seq(),
                                symbol: c.api_symbol().clone(),
                                currency_code: c.currency_code().clone(),
                                market_alias: String::new(),
                            })
                            .collect()
                    })
            },
            async move |price_map| mysql_for_update.modify_crypto_price_bulk(&price_map).await,
            async |item: &PriceFetchItem| twelve_data_api::fetch_symbol_price(&item.symbol).await,
        )
        .await?;

        Ok(())
    }

    // Fetches per-asset-type amount maps (stock/crypto/cash/deposit/saving) keyed by
    // user_seq, for a single currency and user_seq batch. Shared by
    // `sync_current_asset_total` (per-currency breakdown) and
    // `sync_asset_total_summary` (KRW-converted grand total).
    async fn fetch_asset_type_amounts(
        mysql_service: &Arc<M>,
        currency_code: &str,
        user_seqs: &[i64],
        offset: u64,
        caller: &str,
    ) -> anyhow::Result<AssetTypeAmounts> {
        let stock: HashMap<i64, Decimal> = mysql_service
            .find_stock_asset_amount_batch(currency_code, user_seqs)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] find_stock_asset_amount_batch failed \
                     (currency={}, offset={}): {:#}",
                    caller, currency_code, offset, e
                );
            })
            .map(to_amount_map)?;

        let crypto: HashMap<i64, Decimal> = mysql_service
            .find_crypto_asset_amount_batch(currency_code, user_seqs)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] find_crypto_asset_amount_batch failed \
                     (currency={}, offset={}): {:#}",
                    caller, currency_code, offset, e
                );
            })
            .map(to_amount_map)?;

        let cash: HashMap<i64, Decimal> = mysql_service
            .find_cash_asset_amount_batch(currency_code, user_seqs)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] find_cash_asset_amount_batch failed \
                     (currency={}, offset={}): {:#}",
                    caller, currency_code, offset, e
                );
            })
            .map(to_amount_map)?;

        let deposit: HashMap<i64, Decimal> = mysql_service
            .find_deposit_asset_amount_batch(currency_code, user_seqs)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] find_deposit_asset_amount_batch failed \
                     (currency={}, offset={}): {:#}",
                    caller, currency_code, offset, e
                );
            })
            .map(to_amount_map)?;

        let saving: HashMap<i64, Decimal> = mysql_service
            .find_saving_asset_amount_batch(currency_code, user_seqs)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::{}] find_saving_asset_amount_batch failed \
                     (currency={}, offset={}): {:#}",
                    caller, currency_code, offset, e
                );
            })
            .map(to_amount_map)?;

        Ok(AssetTypeAmounts {
            stock,
            crypto,
            cash,
            deposit,
            saving,
        })
    }

    // Aggregates each user's current asset totals and stores snapshot rows.
    pub(super) async fn sync_current_asset_total(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let markets: Vec<Market> = mysql_service.find_markets().await?;

        let batch_size: u64 = *schedule_item.batch_size() as u64;

        for market in markets {
            let currency: &str = market.currency_code();
            let mut offset: u64 = 0;

            loop {
                // Fetch all users in pages.
                // Each asset step uses IN filter on these user_seqs, so users
                // without a specific asset type are still processed (defaulting to 0).
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

                let AssetTypeAmounts {
                    stock: stock_map,
                    crypto: crypto_map,
                    cash: cash_map,
                    deposit: deposit_map,
                    saving: saving_map,
                } = Self::fetch_asset_type_amounts(
                    mysql_service,
                    currency,
                    &user_seqs,
                    offset,
                    "sync_current_asset_total",
                )
                .await?;

                // Single pass over user_seqs: O(1) HashMap lookups per user,
                // no nested iteration across asset types.
                let now: DateTime<Utc> = Utc::now();
                let zero: Decimal = Decimal::ZERO;

                let snapshots: Vec<UserCurrentAssetSnapshot> = user_seqs
                    .iter()
                    .map(|&uid| {
                        UserCurrentAssetSnapshot::new(
                            0,
                            uid,
                            currency.to_owned(),
                            now,
                            cash_map.get(&uid).copied().unwrap_or(zero),
                            stock_map.get(&uid).copied().unwrap_or(zero),
                            crypto_map.get(&uid).copied().unwrap_or(zero),
                            deposit_map.get(&uid).copied().unwrap_or(zero),
                            saving_map.get(&uid).copied().unwrap_or(zero),
                            now,
                            None,
                            "SYSTEM".to_owned(),
                            None,
                        )
                    })
                    .collect();
                
                let batch_snapshots: Vec<user_current_asset_snapshot::ActiveModel> =
                    snapshots.into_iter().map(Into::into).collect();

                mysql_service
                    .input_user_current_asset_snapshot_bulk(batch_snapshots)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_current_asset_total] \
                             input_user_current_asset_snapshot_bulk failed (offset={}): {:#}",
                            offset, e
                        );
                    })?;

                offset += batch_size;
            }
        }

        Ok(())
    }


    // Aggregates each user's total assets across all currencies (converted to KRW)
    // and stores one summary row per user.
    pub(super) async fn sync_asset_total_summary(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let currencies: Vec<CurrencyCode> = mysql_service
            .find_all_currency_code()
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_asset_total_summary] \
                     find_all_currency_code failed: {:#}",
                    e
                );
            })?;

        // KRW 기준 환율 맵 (base_currency_code -> snapshot)
        let krw_exchange_rate_map: HashMap<String, CurrencyExchangeRateSnapshot> = mysql_service
            .find_exchange_rate_snapshot_by_target_currency("KRW")
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_asset_total_summary] \
                     find_exchange_rate_snapshot_by_target_currency failed: {:#}",
                    e
                );
            })?
            .into_iter()
            .map(|snapshot| (snapshot.base_currency_code().clone(), snapshot))
            .collect();

        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let mut offset: u64 = 0;

        loop {
            let user_seqs: Vec<i64> = mysql_service
                .find_user_seq_batch(offset, batch_size)
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::sync_asset_total_summary] \
                         find_user_seq_batch failed (offset={}): {:#}",
                        offset, e
                    );
                })?;

            if user_seqs.is_empty() {
                break;
            }

            // Sum of each user's assets across all currencies, converted to KRW.
            let mut user_total_krw_map: HashMap<i64, Decimal> = HashMap::new();

            for currency in &currencies {
                let currency_code: &str = currency.currency_code();

                let exchange_rate_to_krw: Decimal =
                    match krw_exchange_rate_map.get(currency_code) {
                        Some(snapshot) => *snapshot.exchange_rate(),
                        None => {
                            warn!(
                                "[BatchServiceImpl::sync_asset_total_summary] \
                                 No KRW exchange rate found for currency={}, skipping.",
                                currency_code
                            );
                            continue;
                        }
                    };

                let AssetTypeAmounts {
                    stock,
                    crypto,
                    cash,
                    deposit,
                    saving,
                } = Self::fetch_asset_type_amounts(
                    mysql_service,
                    currency_code,
                    &user_seqs,
                    offset,
                    "sync_asset_total_summary",
                )
                .await?;

                for asset_map in [stock, crypto, cash, deposit, saving] {
                    for (user_seq, amount) in asset_map {
                        *user_total_krw_map.entry(user_seq).or_insert(Decimal::ZERO) +=
                            amount * exchange_rate_to_krw;
                    }
                }
            }

            let now: DateTime<Utc> = Utc::now();

            let summaries: Vec<UserAssetSnapshotSummary> = user_total_krw_map
                .into_iter()
                .map(|(user_seq, total_asset_amount)| {
                    UserAssetSnapshotSummary::new(
                        0,
                        user_seq,
                        now,
                        total_asset_amount,
                        now,
                        None,
                        "SYSTEM".to_owned(),
                        None,
                    )
                })
                .collect();

            let batch_summaries: Vec<user_asset_snapshot_summary::ActiveModel> =
                summaries.into_iter().map(Into::into).collect();

            mysql_service
                .input_user_asset_snapshot_summary_bulk(batch_summaries)
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::sync_asset_total_summary] \
                         input_user_asset_snapshot_summary_bulk failed (offset={}): {:#}",
                        offset, e
                    );
                })?;

            offset += batch_size;
        }

        Ok(())
    }

    // async fn sync_current_asset_detail(
    //     schedule_item: &BatchScheduleItem,
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    // ) -> anyhow::Result<()> {

    //     Ok(())
    // }
}
