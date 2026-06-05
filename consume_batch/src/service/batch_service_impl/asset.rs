//! Asset price sync batch jobs.

use rust_decimal::Decimal;
use sea_orm::ActiveValue;

//use crate::api::kis_api::fetch_current_stock_price;
use crate::entity::user_current_asset_snapshot;
use crate::models::{AssetAmount, CryptoPriceHistory, CurrencyExchangeRateSnapshot, FetchedPrice, PriceFetchItem, StockPriceHistory, StockType, batch_schedule::*};
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
        .filter_map(|a| a.asset_sum.map(|sum| (a.user_seq, sum)))
        .collect()
}

// Synchronizes prices for assets that can be fetched by API symbol.
// Returns successfully fetched FetchedPrice entries.
async fn sync_asset_price<F, G, P>(
    batch_size: u64,
    label: &str,
    fetch_fn: F,
    update_fn: G,
    price_fn: P,
) -> anyhow::Result<Vec<FetchedPrice>>
where
    F: AsyncFn(u64, u64) -> anyhow::Result<Vec<PriceFetchItem>>,
    G: AsyncFn(HashMap<i64, Decimal>) -> anyhow::Result<()>,
    P: AsyncFn(&PriceFetchItem) -> anyhow::Result<Decimal>,
{
    let mut offset: u64 = 0;
    let mut total_count: usize = 0;
    let mut success_count: usize = 0;
    let mut fail_count: usize = 0;
    let mut collected: Vec<FetchedPrice> = Vec::new();

    info!(
        "[BatchServiceImpl::{}] Starting price sync (batch_size={}).",
        label, batch_size
    );

    loop {
        let items: Vec<PriceFetchItem> =
            fetch_fn(offset, batch_size).await.inspect_err(|e| {
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
        }

        if !price_map.is_empty() {
            let batch_success: usize = price_map.len();

            for item in &items {
                if let Some(&price) = price_map.get(&item.seq) {
                    collected.push(FetchedPrice {
                        seq: item.seq,
                        symbol: item.symbol.clone(),
                        currency_code: item.currency_code.clone(),
                        name: item.name.clone(),
                        price,
                    });
                }
            }

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

    Ok(collected)
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
    pub(super) async fn sync_currency_exchange_rates(
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

            let exchange_rate: f64 = match twelve_data_api::fetch_exchange_rate(base, target).await
            {
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

    // Synchronizes stock prices in paged batches and records price history to Elasticsearch.
    pub(super) async fn sync_stock_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        redis_service: &Arc<R>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let ms1: Arc<M> = Arc::clone(mysql_service);
        let ms2: Arc<M> = Arc::clone(mysql_service);
        let ms3: Arc<M> = Arc::clone(mysql_service);
        let rs1: Arc<R> = Arc::clone(redis_service);

        let collected: Vec<FetchedPrice> = sync_asset_price(
            batch_size,
            "sync_stock_price",
            async move |offset, limit| {
                ms1.find_stock_batch(offset, limit).await.map(|v| {
                    v.into_iter()
                        .map(|s| PriceFetchItem {
                            seq: *s.stock_seq(),
                            symbol: s.api_symbol().clone(),
                            currency_code: s.currency_code().clone(),
                            market_alias: s.market_alias().clone(),
                            name: s.stock_name().clone(),
                        })
                        .collect()
                })
            },
            async move |price_map| ms2.modify_stock_price_bulk(&price_map).await,
            async move |item: &PriceFetchItem| {
                if item.currency_code == "USD" {
                    kis_api::fetch_current_overseas_stock_price(&item.market_alias, &item.symbol, &rs1, &ms3)
                        .await
                        .map(|dto| *dto.current_price())
                } else {
                    kis_api::fetch_current_stock_price(&item.symbol, &rs1, &ms3)
                        .await
                        .map(|dto| *dto.current_price())
                }
            },
        )
        .await?;

        if collected.is_empty() {
            return Ok(());
        }

        let now: DateTime<Utc> = Utc::now();
        let index_name: String = format!("{}_{}", schedule_item.index_name(), now.format("%Y%m%d"));

        let docs: Vec<StockPriceHistory> = collected
            .into_iter()
            .map(|fp| StockPriceHistory::new(fp.seq, fp.symbol, fp.currency_code, fp.name, fp.price, now))
            .collect();

        let doc_count: usize = docs.len();
        elastic_service
            .input_bulk(&index_name, docs, None)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_stock_price] ES bulk index failed (index={}): {:#}",
                    index_name, e
                );
            })?;

        info!(
            "[BatchServiceImpl::sync_stock_price] Indexed {} stock price history docs to ES (index={}).",
            doc_count, index_name
        );

        Ok(())
    }
    
    // Synchronizes crypto prices in paged batches and records price history to Elasticsearch.
    pub(super) async fn sync_crypto_price(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
        elastic_service: &Arc<E>,
    ) -> anyhow::Result<()> {
        let batch_size: u64 = *schedule_item.batch_size() as u64;
        let ms1: Arc<M> = Arc::clone(mysql_service);
        let ms2: Arc<M> = Arc::clone(mysql_service);

        let collected: Vec<FetchedPrice> = sync_asset_price(
            batch_size,
            "sync_crypto_price",
            async move |offset, limit| {
                ms1.find_crypto_batch(offset, limit).await.map(|v| {
                    v.into_iter()
                        .map(|c| PriceFetchItem {
                            seq: *c.crypto_seq(),
                            symbol: c.api_symbol().clone(),
                            currency_code: c.currency_code().clone(),
                            market_alias: String::new(),
                            name: c.crypto_name().clone(),
                        })
                        .collect()
                })
            },
            async move |price_map| ms2.modify_crypto_price_bulk(&price_map).await,
            async |item: &PriceFetchItem| {
                twelve_data_api::fetch_crypto_price(&item.symbol).await
            },
        )
        .await?;

        if collected.is_empty() {
            return Ok(());
        }

        let now: DateTime<Utc> = Utc::now();
        let index_name: String = format!("{}_{}", schedule_item.index_name(), now.format("%Y%m%d"));

        let docs: Vec<CryptoPriceHistory> = collected
            .into_iter()
            .map(|fp| CryptoPriceHistory::new(fp.seq, fp.symbol, fp.currency_code, fp.name, fp.price, now))
            .collect();

        let doc_count: usize = docs.len();
        elastic_service
            .input_bulk(&index_name, docs, None)
            .await
            .inspect_err(|e| {
                error!(
                    "[BatchServiceImpl::sync_crypto_price] ES bulk index failed (index={}): {:#}",
                    index_name, e
                );
            })?;

        info!(
            "[BatchServiceImpl::sync_crypto_price] Indexed {} crypto price history docs to ES (index={}).",
            doc_count, index_name
        );
        
        Ok(())
    }
    
    // Aggregates each user's current asset totals and stores snapshot rows.
    pub(super) async fn sync_current_asset_total(
        schedule_item: &BatchScheduleItem,
        mysql_service: &Arc<M>,
    ) -> anyhow::Result<()> {
        let stock_types: Vec<StockType> = mysql_service.find_stock_types().await?;

        let batch_size: u64 = *schedule_item.batch_size() as u64;

        for s_type in stock_types {
            let currency: &str = s_type.currency_code();
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

                // 1. Get stock asset
                let stock_map: HashMap<i64, Decimal> = mysql_service
                    .find_stock_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_current_asset_total] \
                             find_stock_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })
                    .map(to_amount_map)?;

                // 2. Get crypto asset
                let crypto_map: HashMap<i64, Decimal> = mysql_service
                    .find_crypto_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_current_asset_total] \
                             find_crypto_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })
                    .map(to_amount_map)?;

                // 3. Get cash asset
                let cash_map: HashMap<i64, Decimal> = mysql_service
                    .find_cash_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::find_cash_asset_amount_batch] \
                             find_cash_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })
                    .map(to_amount_map)?;

                // 4. Get deposit asset
                let deposit_map: HashMap<i64, Decimal> = mysql_service
                    .find_deposit_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::find_deposit_asset_amount_batch] \
                             find_deposit_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })
                    .map(to_amount_map)?;

                // 5. Get saving asset
                let saving_map: HashMap<i64, Decimal> = mysql_service
                    .find_saving_asset_amount_batch(currency, &user_seqs)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::find_deposit_asset_amount_batch] \
                             find_saving_asset_amount_batch failed \
                             (currency={}, offset={}): {:#}",
                            currency, offset, e
                        );
                    })
                    .map(to_amount_map)?;

                // Single pass over user_seqs: O(1) HashMap lookups per user,
                // no nested iteration across asset types.
                let now: sea_orm::prelude::DateTime = Utc::now().naive_utc();
                let zero: Decimal = Decimal::ZERO;
                
                let batch_snapshots: Vec<user_current_asset_snapshot::ActiveModel> = user_seqs
                    .iter()
                    .map(|&uid| user_current_asset_snapshot::ActiveModel {
                        summary_seq: ActiveValue::NotSet,
                        user_seq: ActiveValue::Set(uid),
                        currency_code: ActiveValue::Set(currency.to_owned()),
                        aggregated_at: ActiveValue::Set(now),
                        stock_amount: ActiveValue::Set(
                            stock_map.get(&uid).copied().unwrap_or(zero),
                        ),
                        crypto_amount: ActiveValue::Set(
                            crypto_map.get(&uid).copied().unwrap_or(zero),
                        ),
                        cash_amount: ActiveValue::Set(cash_map.get(&uid).copied().unwrap_or(zero)),
                        deposit_amount: ActiveValue::Set(
                            deposit_map.get(&uid).copied().unwrap_or(zero),
                        ),
                        saving_amount: ActiveValue::Set(
                            saving_map.get(&uid).copied().unwrap_or(zero),
                        ),
                        created_at: ActiveValue::Set(now),
                        updated_at: ActiveValue::NotSet,
                        created_by: ActiveValue::Set("SYSTEM".to_owned()),
                        updated_by: ActiveValue::NotSet,
                    })
                    .collect();

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

    // async fn sync_current_asset_detail(
    //     schedule_item: &BatchScheduleItem,
    //     mysql_service: &Arc<M>,
    //     elastic_service: &Arc<E>,
    // ) -> anyhow::Result<()> {




    //     Ok(())
    // }

}
