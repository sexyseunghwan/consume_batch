use crate::common::*;
use crate::entity::{
    agg_group, common_consume_keyword_type, common_consume_prodt_keyword, crypto,
    currency_exchange_rate_snapshot, send_email_agg_group, spent_detail, spent_detail_indexing,
    stock, telegram_room, user_payment_methods, users,
};
use crate::models::{
    Crypto, CurrencyExchangeRateSnapshot, SendEmailAggGroup, SpentDetail, SpentDetailIndexing,
    SpentDetailWithRelations, SpentTypeKeyword, Stock,
};
use crate::repository::mysql_repository::MysqlRepository;
use sea_orm::{JoinType, QuerySelect, RelationTrait};

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
            .select_only()
            .column(common_consume_keyword_type::Column::ConsumeKeywordTypeId)
            .column(common_consume_prodt_keyword::Column::ConsumeKeyword)
            .column(common_consume_keyword_type::Column::ConsumeKeywordType)
            .column(common_consume_prodt_keyword::Column::KeywordWeight)
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
            .select_only()
            .column(spent_detail::Column::SpentIdx)
            .column(spent_detail::Column::SpentName)
            .column(spent_detail::Column::SpentMoney)
            .column(spent_detail::Column::SpentAt)
            .column(spent_detail::Column::CreatedAt)
            .column(spent_detail::Column::UserSeq)
            .column(common_consume_keyword_type::Column::ConsumeKeywordTypeId)
            .column(common_consume_keyword_type::Column::ConsumeKeywordType)
            .column(spent_detail::Column::RoomSeq)
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
            .select_only()
            .column(spent_detail_indexing::Column::SpentIdx)
            .column(spent_detail_indexing::Column::SpentName)
            .column(spent_detail_indexing::Column::SpentMoney)
            .column(spent_detail_indexing::Column::SpentAt)
            .column(spent_detail_indexing::Column::CreatedAt)
            .column(spent_detail_indexing::Column::UserSeq)
            .column(spent_detail_indexing::Column::ConsumeKeywordTypeId)
            .column(spent_detail_indexing::Column::ConsumeKeywordType)
            .column(spent_detail_indexing::Column::RoomSeq)
            .column(spent_detail_indexing::Column::UserId)
            .column(spent_detail_indexing::Column::CardAlias)
            .column(spent_detail_indexing::Column::UpdatedAt)
            .column(spent_detail_indexing::Column::UpdatedBy)
            .column(spent_detail_indexing::Column::AggGroupSeq)
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
            .select_only()
            .column(spent_detail_indexing::Column::SpentIdx)
            .column(spent_detail_indexing::Column::SpentName)
            .column(spent_detail_indexing::Column::SpentMoney)
            .column(spent_detail_indexing::Column::SpentAt)
            .column(spent_detail_indexing::Column::CreatedAt)
            .column(spent_detail_indexing::Column::UserSeq)
            .column(spent_detail_indexing::Column::ConsumeKeywordTypeId)
            .column(spent_detail_indexing::Column::ConsumeKeywordType)
            .column(spent_detail_indexing::Column::RoomSeq)
            .column(spent_detail_indexing::Column::UserId)
            .column(spent_detail_indexing::Column::CardAlias)
            .column(spent_detail_indexing::Column::UpdatedAt)
            .column(spent_detail_indexing::Column::UpdatedBy)
            .column(spent_detail_indexing::Column::AggGroupSeq)
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
            .select_only()
            .column(spent_detail::Column::SpentIdx)
            .column(spent_detail::Column::SpentName)
            .column(spent_detail::Column::SpentMoney)
            .column(spent_detail::Column::SpentAt)
            .column(spent_detail::Column::ShouldIndex)
            .column(spent_detail::Column::UserSeq)
            .column(spent_detail::Column::SpentGroupId)
            .column(spent_detail::Column::ConsumeKeywordTypeId)
            .column(spent_detail::Column::RoomSeq)
            .column(spent_detail::Column::PaymentMethodId)
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
            .select_only()
            .column(send_email_agg_group::Column::AggGroupSeq)
            .column(send_email_agg_group::Column::EmailId)
            .column(send_email_agg_group::Column::IsActive)
            .column(send_email_agg_group::Column::CreatedAt)
            .column(send_email_agg_group::Column::UpdatedAt)
            .column(send_email_agg_group::Column::CreatedBy)
            .column(send_email_agg_group::Column::UpdatedBy)
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
                .select_only()
                .column(currency_exchange_rate_snapshot::Column::ExchangeRateSnapshotSeq)
                .column(currency_exchange_rate_snapshot::Column::BaseCurrencyCode)
                .column(currency_exchange_rate_snapshot::Column::TargetCurrencyCode)
                .column(currency_exchange_rate_snapshot::Column::BaseAmount)
                .column(currency_exchange_rate_snapshot::Column::ExchangeRate)
                .column(currency_exchange_rate_snapshot::Column::IsActive)
                .column(currency_exchange_rate_snapshot::Column::CreatedAt)
                .column(currency_exchange_rate_snapshot::Column::UpdatedAt)
                .column(currency_exchange_rate_snapshot::Column::CreatedBy)
                .column(currency_exchange_rate_snapshot::Column::UpdatedBy)
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
            .select_only()
            .column(stock::Column::StockSeq)
            .column(stock::Column::MarketSeq)
            .column(stock::Column::StockName)
            .column(stock::Column::ApiSymbol)
            .column(stock::Column::StockPrice)
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
            .select_only()
            .column(crypto::Column::CryptoSeq)
            .column(crypto::Column::CryptoName)
            .column(crypto::Column::CryptoPrice)
            .column(crypto::Column::ApiSymbol)
            .column(crypto::Column::CurrencyCode)
            .column(crypto::Column::CreatedAt)
            .column(crypto::Column::UpdatedAt)
            .column(crypto::Column::CreatedBy)
            .column(crypto::Column::UpdatedBy)
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
}
