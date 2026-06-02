use rust_decimal::Decimal;

use crate::common::*;
use crate::entity::{
    crypto, currency_exchange_rate_snapshot, spent_detail, spent_detail_indexing, stock,
};
use crate::repository::mysql_repository::MysqlRepository;
use sea_orm::sea_query::{CaseStatement, Expr};

use super::MysqlServiceImpl;

impl<R: MysqlRepository + Send + Sync> MysqlServiceImpl<R> {
    pub(super) async fn modify_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
        batch_size: usize,
    ) -> anyhow::Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_batch] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let mut total_affected: u64 = 0;

        let result: std::result::Result<(), anyhow::Error> = async {
            for chunk in updates.chunks(batch_size) {
                let mut case_stmt: CaseStatement = CaseStatement::new();
                let mut ids: Vec<i64> = Vec::with_capacity(chunk.len());

                for (spent_idx, new_type_id) in chunk {
                    case_stmt = case_stmt.case(
                        Expr::col(spent_detail::Column::SpentIdx).eq(*spent_idx),
                        Expr::value(*new_type_id),
                    );
                    ids.push(*spent_idx);
                }

                let result: sea_orm::UpdateResult = spent_detail::Entity::update_many()
                    .col_expr(spent_detail::Column::ConsumeKeywordTypeId, case_stmt.into())
                    .filter(spent_detail::Column::SpentIdx.is_in(ids))
                    .exec(&txn)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[MysqlServiceImpl::modify_spent_detail_type_batch] Failed to bulk update: {:#}",
                            e
                        );
                    })?;

                total_affected += result.rows_affected;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_batch] Rolling back transaction: {}",
                e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::modify_spent_detail_type_batch] Failed to rollback: {:#}",
                    e
                );
            })?;
            return Err(e);
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_batch] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(total_affected)
    }

    pub(super) async fn modify_spent_detail_indexing_type_batch(
        &self,
        updates: Vec<(i64, i64, String)>,
        batch_size: usize,
    ) -> anyhow::Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing_type_batch] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let mut total_affected: u64 = 0;

        let result: std::result::Result<(), anyhow::Error> = async {
            for chunk in updates.chunks(batch_size) {
                let mut case_id_stmt: CaseStatement = CaseStatement::new();
                let mut case_name_stmt: CaseStatement = CaseStatement::new();
                let mut ids: Vec<i64> = Vec::with_capacity(chunk.len());

                for (spent_idx, new_type_id, new_type_name) in chunk {
                    case_id_stmt = case_id_stmt.case(
                        Expr::col(spent_detail_indexing::Column::SpentIdx).eq(*spent_idx),
                        Expr::value(*new_type_id),
                    );
                    case_name_stmt = case_name_stmt.case(
                        Expr::col(spent_detail_indexing::Column::SpentIdx).eq(*spent_idx),
                        Expr::value(new_type_name.clone()),
                    );
                    ids.push(*spent_idx);
                }

                let result: sea_orm::UpdateResult = spent_detail_indexing::Entity::update_many()
                    .col_expr(
                        spent_detail_indexing::Column::ConsumeKeywordTypeId,
                        case_id_stmt.into(),
                    )
                    .col_expr(
                        spent_detail_indexing::Column::ConsumeKeywordType,
                        case_name_stmt.into(),
                    )
                    .filter(spent_detail_indexing::Column::SpentIdx.is_in(ids))
                    .exec(&txn)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[MysqlServiceImpl::modify_spent_detail_indexing_type_batch] Failed to bulk update: {:#}",
                            e
                        );
                    })?;

                total_affected += result.rows_affected;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing_type_batch] Rolling back: {}",
                e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::modify_spent_detail_indexing_type_batch] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e);
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing_type_batch] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(total_affected)
    }

    #[allow(dead_code)]
    pub(super) async fn modify_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64> {
        if updates.is_empty() {
            return Ok(0);
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_one_by_one] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let mut total_affected: u64 = 0;

        let result: std::result::Result<(), anyhow::Error> = async {
            for (spent_idx, new_type_id) in &updates {
                let result: sea_orm::UpdateResult = spent_detail::Entity::update_many()
                    .col_expr(
                        spent_detail::Column::ConsumeKeywordTypeId,
                        Expr::value(*new_type_id),
                    )
                    .filter(spent_detail::Column::SpentIdx.eq(*spent_idx))
                    .exec(&txn)
                    .await
                    .inspect_err(|e| {
                        error!(
                            "[MysqlServiceImpl::modify_spent_detail_type_one_by_one] Failed to update row: {:#}",
                            e
                        );
                    })?;
                total_affected += result.rows_affected;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_one_by_one] Rolling back: {}",
                e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::modify_spent_detail_type_one_by_one] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e);
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_type_one_by_one] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(total_affected)
    }

    pub(super) async fn modify_currency_exchange_rate_snapshot_bulk(
        &self,
        snapshot_map: &HashMap<i64, f64>,
    ) -> anyhow::Result<()> {
        if snapshot_map.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();
        let now: chrono::NaiveDateTime = Utc::now().naive_utc();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_currency_exchange_rate_snapshot_bulk] Failed to begin transaction: {:#}",
                e
            );
        })?;

        for (seq, rate) in snapshot_map {
            let decimal_rate = rust_decimal::Decimal::from_f64_retain(*rate).ok_or_else(|| {
                anyhow!(
                    "Failed to convert f64 rate {} to Decimal for seq={}",
                    rate,
                    seq
                )
            })?;

            currency_exchange_rate_snapshot::Entity::update_many()
                .col_expr(
                    currency_exchange_rate_snapshot::Column::ExchangeRate,
                    Expr::value(decimal_rate),
                )
                .col_expr(
                    currency_exchange_rate_snapshot::Column::UpdatedAt,
                    Expr::value(now),
                )
                .col_expr(
                    currency_exchange_rate_snapshot::Column::UpdatedBy,
                    Expr::value("batch"),
                )
                .filter(
                    currency_exchange_rate_snapshot::Column::ExchangeRateSnapshotSeq.eq(*seq),
                )
                .exec(&txn)
                .await
                .inspect_err(|e| {
                    error!(
                        "[MysqlServiceImpl::modify_currency_exchange_rate_snapshot_bulk] UPDATE failed for seq={}: {:#}",
                        seq, e
                    );
                })?;
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_currency_exchange_rate_snapshot_bulk] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(())
    }

    pub(super) async fn modify_stock_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()> {
        if price_map.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();
        let now: chrono::NaiveDateTime = Utc::now().naive_utc();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_stock_price_bulk] Failed to begin transaction: {:#}",
                e
            );
        })?;

        for (seq, price) in price_map {
            stock::Entity::update_many()
                .col_expr(stock::Column::StockPrice, Expr::value(*price))
                .col_expr(stock::Column::UpdatedAt, Expr::value(now))
                .col_expr(stock::Column::UpdatedBy, Expr::value("batch"))
                .filter(stock::Column::StockSeq.eq(*seq))
                .exec(&txn)
                .await
                .inspect_err(|e| {
                    error!(
                        "[MysqlServiceImpl::modify_stock_price_bulk] UPDATE failed for seq={}: {:#}",
                        seq, e
                    );
                })?;
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_stock_price_bulk] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(())
    }

    pub(super) async fn modify_crypto_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()> {
        if price_map.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();
        let now: chrono::NaiveDateTime = Utc::now().naive_utc();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_crypto_price_bulk] Failed to begin transaction: {:#}",
                e
            );
        })?;

        for (seq, price) in price_map {
            crypto::Entity::update_many()
                .col_expr(crypto::Column::CryptoPrice, Expr::value(*price))
                .col_expr(crypto::Column::UpdatedAt, Expr::value(now))
                .col_expr(crypto::Column::UpdatedBy, Expr::value("batch"))
                .filter(crypto::Column::CryptoSeq.eq(*seq))
                .exec(&txn)
                .await
                .inspect_err(|e| {
                    error!(
                        "[MysqlServiceImpl::modify_crypto_price_bulk] UPDATE failed for seq={}: {:#}",
                        seq, e
                    );
                })?;
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_crypto_price_bulk] Failed to commit: {:#}",
                e
            );
        })?;

        Ok(())
    }
}
