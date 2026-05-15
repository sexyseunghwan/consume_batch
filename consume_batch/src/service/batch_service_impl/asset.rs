//! Populate the `DIM_CALENDAR` date dimension table.

use rust_decimal::Decimal;

use crate::models::{CurrencyExchangeRateSnapshot, batch_schedule::*, Stock};
use crate::service_trait::{
    consume_service::ConsumeService, elastic_service::ElasticService,
    indexing_service::IndexingService, mysql_service::MysqlService,
    producer_service::ProducerService, public_data_service::PublicDataService,
    smtp_service::SmtpService,
};
use crate::{batch_log, common::*};

use crate::api::twelve_data_api::*;

use super::BatchServiceImpl;

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
                error!("[BatchServiceImpl::sync_currency_exchange_rates] Error at `target_currency_infos`: {:#}",e);
            })?;
        
        if currency_snapshots.is_empty() {
            info!("[BatchServiceImpl::sync_currency_exchange_rates] No snapshot rows found, skipping.");
            return Ok(())
        }

        let mut snapshot_map: HashMap<i64, f64> = HashMap::new();

        for snapshot in currency_snapshots {
            let base: &str = snapshot.base_currency_code();
            let target: &str = snapshot.target_currency_code();
            let seq: i64 = snapshot.exchange_rate_snapshot_seq;

            let exchange_rate: f64 = match fetch_exchange_rate(base, target).await {
                Ok(r) => r,
                Err(e) => {
                    error!("[BatchServiceImpl::sync_currency_exchange_rates] Error at `exchange_rate`: {:#}", e);
                    continue;
                }
            };
            
            snapshot_map.insert(seq, exchange_rate);
        }

        if snapshot_map.is_empty() {
            warn!("[BatchServiceImpl::sync_currency_exchange_rates] All API fetches failed, skipping DB update.");
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
        let mut offset: u64 = 0;
        let mut total_count: usize = 0;
        let mut success_count: usize = 0;
        let mut fail_count: usize = 0;

        batch_log!(
            info,
            "[BatchServiceImpl::sync_stock_price] Starting stock price sync (batch_size={}).",
            batch_size
        );

        loop {
            let stocks: Vec<Stock> = mysql_service
                .find_stock_batch(offset, batch_size)
                .await
                .inspect_err(|e| {
                    error!(
                        "[BatchServiceImpl::sync_stock_price] Failed to fetch stock batch (offset={}): {:#}",
                        offset, e
                    );
                })?;

            if stocks.is_empty() {
                break;
            }
            
            total_count += stocks.len();
            let mut price_map: HashMap<i64, Decimal> = HashMap::new();

            for stock in &stocks {
                let seq: i64 = *stock.stock_seq();
                let symbol: &str = stock.api_symbol();

                match fetch_stock_price(symbol).await {
                    Ok(price) => {
                        price_map.insert(seq, price);
                    }
                    Err(e) => {
                        error!(
                            "[BatchServiceImpl::sync_stock_price] Failed to fetch price for {} (seq={}): {:#}",
                            symbol, seq, e
                        );
                        fail_count += 1;
                    }
                }
            }

            if !price_map.is_empty() {
                let batch_success: usize = price_map.len();
                mysql_service
                    .modify_stock_price_bulk(&price_map)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[BatchServiceImpl::sync_stock_price] Bulk update failed (offset={}): {:#}",
                            offset, e
                        );
                    })?;
                success_count += batch_success;
            }

            offset += batch_size;
        }

        batch_log!(
            info,
            "[BatchServiceImpl::sync_stock_price] Completed: total={}, success={}, failed={}.",
            total_count, success_count, fail_count
        );

        Ok(())
    }
}
