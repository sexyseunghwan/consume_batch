use crate::common::*;
use crate::entity::{
    agg_group, cash_asset, common_consume_keyword_type, common_consume_prodt_keyword, crypto,
    crypto_asset, currency_code, currency_exchange_rate_snapshot, deposit_asset, kis_api_token,
    saving_asset, send_email_agg_group, spent_detail, spent_detail_indexing, stock, stock_asset,
    stock_type, telegram_room, user_payment_methods, users,
};
use crate::models::{
    AssetAmount, CashAsset, Crypto, CurrencyExchangeRateSnapshot, DepositAsset, KisApiToken,
    SavingAsset, SendEmailAggGroup, SpentDetail, SpentDetailIndexing, SpentDetailWithRelations,
    SpentTypeKeyword, Stock, StockType,
};
use crate::repository::mysql_repository::MysqlRepository;
use sea_orm::sea_query::{Expr, Func, SimpleExpr};
use sea_orm::{EntityOrSelect, JoinType, PaginatorTrait, QuerySelect, RelationTrait};

use super::MysqlServiceImpl;

impl<R: MysqlRepository + Send + Sync> MysqlServiceImpl<R> {
    pub(super) async fn find_spent_type_keywords_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentTypeKeyword>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentTypeKeyword> = common_consume_prodt_keyword::Entity::find()
            .join(
                JoinType::InnerJoin,
                common_consume_prodt_keyword::Relation::CommonConsumeKeywordType.def(),
            )
            .select()
            .column(common_consume_keyword_type::Column::ConsumeKeywordType)
            .offset(offset)
            .limit(limit)
            .into_model::<SpentTypeKeyword>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_spent_type_keywords_batch] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_spent_details_for_indexing(
        &self,
        spent_idxs: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>> {
        if spent_idxs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentDetailWithRelations> = spent_detail::Entity::find()
            .join(JoinType::InnerJoin, spent_detail::Relation::CommonConsumeKeywordType.def())
            .join(JoinType::InnerJoin, spent_detail::Relation::Users.def())
            .join(JoinType::InnerJoin, spent_detail::Relation::TelegramRoom.def())
            .join(JoinType::InnerJoin, spent_detail::Relation::UserPaymentMethods.def())
            .join(JoinType::InnerJoin, telegram_room::Relation::AggGroup.def())
            .select()
            .column(common_consume_keyword_type::Column::ConsumeKeywordType)
            .column(users::Column::UserId)
            .column(user_payment_methods::Column::CardAlias)
            .column(agg_group::Column::AggGroupSeq)
            .filter(spent_detail::Column::ShouldIndex.eq(1))
            .filter(spent_detail::Column::SpentIdx.is_in(spent_idxs.to_vec()))
            .filter(telegram_room::Column::IsRoomApproved.eq(true))
            .filter(agg_group::Column::IsActive.eq(true))
            .into_model::<SpentDetailWithRelations>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_spent_details_for_indexing] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_spent_detail_indexing_by_ids(
        &self,
        ids: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailIndexing>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentDetailIndexing> = spent_detail_indexing::Entity::find()
            .select()
            .filter(spent_detail_indexing::Column::SpentIdx.is_in(ids.to_vec()))
            .into_model::<SpentDetailIndexing>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_spent_detail_indexing_by_ids] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_spent_detail_indexing_for_index(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailIndexing>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentDetailIndexing> = spent_detail_indexing::Entity::find()
            .select()
            .offset(offset)
            .limit(limit)
            .order_by_asc(spent_detail_indexing::Column::SpentIdx)
            .into_model::<SpentDetailIndexing>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_spent_detail_indexing_for_index] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_spent_details(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetail>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentDetail> = spent_detail::Entity::find()
            .select()
            .offset(offset)
            .limit(limit)
            .order_by_asc(spent_detail::Column::SpentIdx)
            .into_model::<SpentDetail>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_spent_details] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_send_email_agg_group(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SendEmailAggGroup>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SendEmailAggGroup> = send_email_agg_group::Entity::find()
            .select()
            .filter(send_email_agg_group::Column::IsActive.eq(true))
            .offset(offset)
            .limit(limit)
            .order_by_asc(send_email_agg_group::Column::AggGroupSeq)
            .into_model::<SendEmailAggGroup>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_send_email_agg_group] Failed to execute query (offset={}, limit={}): {:#}",
                    offset, limit, e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_currency_exchange_rate_snapshot(
        &self,
    ) -> anyhow::Result<Vec<CurrencyExchangeRateSnapshot>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Vec<CurrencyExchangeRateSnapshot> =
            currency_exchange_rate_snapshot::Entity::find()
                .select()
                .filter(currency_exchange_rate_snapshot::Column::IsActive.eq(true))
                .into_model::<CurrencyExchangeRateSnapshot>()
                .all(db)
                .await
                .inspect_err(|e| {
                    error!(
                        "[MysqlServiceImpl::find_currency_exchange_rate_snapshot] Failed to execute query: {:#}",
                        e
                    );
                })?;

        Ok(result)
    }

    pub(super) async fn find_stock_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<Stock>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<Stock> = stock::Entity::find()
            .join(JoinType::InnerJoin, stock::Relation::StockType.def())
            .select_only()
            .column(stock::Column::StockSeq)
            .column(stock::Column::MarketSeq)
            .column(stock::Column::StockName)
            .column(stock::Column::ApiSymbol)
            .column(stock::Column::StockPrice)
            .column(stock_type::Column::CurrencyCode)
            .column(stock::Column::CreatedAt)
            .column(stock::Column::UpdatedAt)
            .column(stock::Column::CreatedBy)
            .column(stock::Column::UpdatedBy)
            .order_by_asc(stock::Column::StockSeq)
            .offset(offset)
            .limit(limit)
            .into_model::<Stock>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_stock_batch] Failed to execute query (offset={}, limit={}): {:#}",
                    offset, limit, e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_crypto_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<Crypto>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<Crypto> = crypto::Entity::find()
            .select()
            .order_by_asc(crypto::Column::CryptoSeq)
            .offset(offset)
            .limit(limit)
            .into_model::<Crypto>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_crypto_batch] Failed to execute query (offset={}, limit={}): {:#}",
                    offset, limit, e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_stock_types(&self) -> anyhow::Result<Vec<StockType>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Vec<StockType> = stock_type::Entity::find()
            .select()
            .into_model::<StockType>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_stock_types] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(result)
    }

    pub(super) async fn find_user_seq_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<i64>> {
        #[derive(FromQueryResult)]
        struct UserSeqRow {
            user_seq: i64,
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let rows: Vec<UserSeqRow> = users::Entity::find()
            .select_only()
            .column(users::Column::UserSeq)
            .order_by_asc(users::Column::UserSeq)
            .offset(offset)
            .limit(limit)
            .into_model::<UserSeqRow>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_user_seq_batch] Failed to execute query \
                     (offset={}, limit={}): {:#}",
                    offset, limit, e
                );
            })?;

        Ok(rows.into_iter().map(|r| r.user_seq).collect())
    }

    pub(super) async fn find_stock_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>> {
        if user_seqs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<AssetAmount> = stock_asset::Entity::find()
            .join(JoinType::InnerJoin, stock_asset::Relation::Stock.def())
            .join(JoinType::InnerJoin, stock::Relation::StockType.def())
            .select_only()
            .column(stock_asset::Column::UserSeq)
            .column_as(
                SimpleExpr::from(Func::sum(
                    Expr::col(stock_asset::Column::StockCnt)
                        .mul(Expr::col(stock::Column::StockPrice)),
                )),
                "asset_sum",
            )
            .filter(stock_type::Column::CurrencyCode.eq(currency_code))
            .filter(stock_asset::Column::UserSeq.is_in(user_seqs.to_vec()))
            .group_by(stock_asset::Column::UserSeq)
            .into_model::<AssetAmount>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_stock_asset_amount_batch] Failed to execute query \
                     (currency_code={}): {:#}",
                    currency_code, e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_crypto_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>> {
        if user_seqs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<AssetAmount> = crypto_asset::Entity::find()
            .join(JoinType::InnerJoin, crypto_asset::Relation::Crypto.def())
            .join(JoinType::InnerJoin, crypto::Relation::CurrencyCode.def())
            .select_only()
            .column(crypto_asset::Column::UserSeq)
            .column_as(
                SimpleExpr::from(Func::sum(
                    Expr::col(crypto_asset::Column::CryptoCnt)
                        .mul(Expr::col(crypto::Column::CryptoPrice)),
                )),
                "asset_sum",
            )
            .filter(crypto::Column::CurrencyCode.eq(currency_code))
            .filter(crypto_asset::Column::UserSeq.is_in(user_seqs.to_vec()))
            .group_by(crypto_asset::Column::UserSeq)
            .into_model::<AssetAmount>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_crypto_asset_amount_batch] Failed to execute query \
                     (currency_code={}): {:#}",
                    currency_code, e
                );
            })?;

        Ok(results)
    }

    pub(super) async fn find_cash_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>> {
        if user_seqs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Vec<AssetAmount> = cash_asset::Entity::find()
            // .join(
            //     JoinType::InnerJoin,
            //     cash_asset::Relation::CurrencyCode.def(),
            // )
            .select_only()
            .column(cash_asset::Column::UserSeq)
            .column_as(
                SimpleExpr::from(Func::sum(Expr::col(cash_asset::Column::Cash))),
                "asset_sum",
            )
            .filter(cash_asset::Column::CurrencyCode.eq(currency_code))
            .filter(cash_asset::Column::UserSeq.is_in(user_seqs.to_vec()))
            .group_by(cash_asset::Column::UserSeq)
            .into_model::<AssetAmount>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_cash_asset_amount_batch] Failed to execute query \
                     (currency_code={}): {:#}",
                    currency_code, e
                );
            })?;

        Ok(result)
    }

    pub(super) async fn find_deposit_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>> {
        if user_seqs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Vec<AssetAmount> = deposit_asset::Entity::find()
            .select_only()
            .column(deposit_asset::Column::UserSeq)
            .column_as(
                SimpleExpr::from(Func::sum(Expr::col(deposit_asset::Column::DepositAmount))),
                "asset_sum",
            )
            .filter(deposit_asset::Column::CurrencyCode.eq(currency_code))
            .filter(deposit_asset::Column::UserSeq.is_in(user_seqs.to_vec()))
            .group_by(deposit_asset::Column::UserSeq)
            .into_model::<AssetAmount>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_cash_asset_amount_batch] Failed to execute query \
                     (currency_code={}): {:#}",
                    currency_code, e
                );
            })?;

        Ok(result)
    }

    pub(super) async fn find_saving_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>> {
        if user_seqs.is_empty() {
            return Ok(Vec::new());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Vec<AssetAmount> = saving_asset::Entity::find()
            .select_only()
            .column(saving_asset::Column::UserSeq)
            .column_as(
                SimpleExpr::from(Func::sum(Expr::col(
                    saving_asset::Column::AccumSavingAmount,
                ))),
                "asset_sum",
            )
            .filter(saving_asset::Column::CurrencyCode.eq(currency_code))
            .filter(saving_asset::Column::UserSeq.is_in(user_seqs.to_vec()))
            .group_by(saving_asset::Column::UserSeq)
            .into_model::<AssetAmount>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_saving_asset_amount_batch] Failed to execute query \
                     (currency_code={}): {:#}",
                    currency_code, e
                );
            })?;

        Ok(result)
    }

    pub(super) async fn find_kis_api_token(&self) -> anyhow::Result<Option<KisApiToken>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let result: Option<KisApiToken> = kis_api_token::Entity::find()
            .into_model::<KisApiToken>()
            .one(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::find_kis_api_token] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(result)
    }
}
