//! MySQL service implementation.
//!
//! # Submodule Layout
//!
//! | File          | Responsibility                              |
//! |---------------|---------------------------------------------|
//! | `mod.rs`      | Struct definition, `MysqlService` trait impl |
//! | `select.rs`   | All `find_*` query methods                  |
//! | `insert.rs`   | Bulk insert / upsert methods                |
//! | `update.rs`   | All `modify_*` update methods               |
//! | `delete.rs`   | All `delete_*` methods                      |

mod delete;
mod insert;
mod select;
mod update;

use rust_decimal::Decimal;

use crate::common::*;
use crate::entity::dim_calendar;
use crate::models::{
    Crypto, CurrencyExchangeRateSnapshot, SendEmailAggGroup, SpentDetail, SpentDetailIndexing,
    SpentDetailWithRelations, SpentTypeKeyword, Stock, StockAssetAmount, StockType,
};
use crate::repository::mysql_repository::MysqlRepository;
use crate::service_trait::mysql_service::MysqlService;

#[derive(Debug, Getters, Clone, new)]
pub struct MysqlServiceImpl<R: MysqlRepository> {
    pub(super) db_conn: R,
}

#[async_trait]
impl<R> MysqlService for MysqlServiceImpl<R>
where
    R: MysqlRepository + Send + Sync,
{
    async fn find_spent_type_keywords_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentTypeKeyword>> {
        self.find_spent_type_keywords_batch(offset, limit).await
    }

    async fn find_spent_details_for_indexing(
        &self,
        spent_idxs: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>> {
        self.find_spent_details_for_indexing(spent_idxs).await
    }

    async fn find_spent_detail_indexing_by_ids(
        &self,
        ids: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailIndexing>> {
        self.find_spent_detail_indexing_by_ids(ids).await
    }

    async fn find_spent_detail_indexing_for_index(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailIndexing>> {
        self.find_spent_detail_indexing_for_index(offset, limit)
            .await
    }

    async fn find_spent_details(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetail>> {
        self.find_spent_details(offset, limit).await
    }

    async fn modify_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
        batch_size: usize,
    ) -> anyhow::Result<u64> {
        self.modify_spent_detail_type_batch(updates, batch_size)
            .await
    }

    async fn modify_spent_detail_indexing_type_batch(
        &self,
        updates: Vec<(i64, i64, String)>,
        batch_size: usize,
    ) -> anyhow::Result<u64> {
        self.modify_spent_detail_indexing_type_batch(updates, batch_size)
            .await
    }

    async fn modify_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64> {
        self.modify_spent_detail_type_one_by_one(updates).await
    }

    async fn input_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()> {
        self.input_dim_calendar_bulk(rows).await
    }

    async fn modify_spent_detail_indexing(
        &self,
        upsert_list: Vec<SpentDetailWithRelations>,
    ) -> anyhow::Result<()> {
        self.modify_spent_detail_indexing(upsert_list).await
    }

    async fn delete_spent_detail_indexing(&self, delete_list: &[i64]) -> anyhow::Result<()> {
        self.delete_spent_detail_indexing(delete_list).await
    }

    async fn find_send_email_agg_group(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SendEmailAggGroup>> {
        self.find_send_email_agg_group(offset, limit).await
    }

    async fn find_currency_exchange_rate_snapshot(
        &self,
    ) -> anyhow::Result<Vec<CurrencyExchangeRateSnapshot>> {
        self.find_currency_exchange_rate_snapshot().await
    }

    async fn modify_currency_exchange_rate_snapshot_bulk(
        &self,
        snapshot_map: &HashMap<i64, f64>,
    ) -> anyhow::Result<()> {
        self.modify_currency_exchange_rate_snapshot_bulk(snapshot_map)
            .await
    }

    async fn find_stock_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<Stock>> {
        self.find_stock_batch(offset, limit).await
    }

    async fn modify_stock_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()> {
        self.modify_stock_price_bulk(price_map).await
    }

    async fn find_crypto_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<Crypto>> {
        self.find_crypto_batch(offset, limit).await
    }

    async fn modify_crypto_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()> {
        self.modify_crypto_price_bulk(price_map).await
    }

    async fn find_stock_types(&self) -> anyhow::Result<Vec<StockType>> {
        self.find_stock_types().await
    }

    async fn find_stock_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<StockAssetAmount>> {
        self.find_stock_asset_amount_batch(currency_code, user_seqs)
            .await
    }

    async fn find_user_seq_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<i64>> {
        self.find_user_seq_batch(offset, limit).await
    }

    async fn find_users_size(&self) -> anyhow::Result<u64> {
        self.find_users_size().await
    }
}
