#![allow(dead_code)]
use rust_decimal::Decimal;

use crate::common::*;
use crate::entity::dim_calendar;
use crate::models::{
    AssetAmount, Crypto, CurrencyExchangeRateSnapshot, SendEmailAggGroup, SpentDetail,
    SpentDetailIndexing, SpentDetailWithRelations, SpentTypeKeyword, Stock, StockType,
};

/// Data access contract for MySQL reads and writes.
///
/// Each `Requested query` section summarizes the SeaORM query builder used by
/// the actual implementation in `mysql_service_impl` as human-readable SQL.
#[async_trait]
pub trait MysqlService {
    /// Fetches consume keywords and their consume types in batches.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   cckt.consume_keyword_type_id,
    ///   ccpk.consume_keyword,
    ///   cckt.consume_keyword_type,
    ///   ccpk.keyword_weight
    /// FROM COMMON_CONSUME_PRODT_KEYWORD ccpk
    /// INNER JOIN COMMON_CONSUME_KEYWORD_TYPE cckt
    ///   ON ccpk.consume_keyword_type_id = cckt.consume_keyword_type_id
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_spent_type_keywords_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentTypeKeyword>>;

    // Deprecated: old offset/limit-based spent detail indexing query.
    // Replaced by find_spent_details_for_indexing, which accepts spent_idx values.
    // async fn find_spent_details_for_indexing(
    //     &self,
    //     offset: u64,
    //     limit: u64,
    // ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// Fetches denormalized indexing data for the provided spent IDs.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   sd.spent_idx,
    ///   sd.spent_name,
    ///   sd.spent_money,
    ///   sd.spent_at,
    ///   sd.created_at,
    ///   sd.user_seq,
    ///   cckt.consume_keyword_type_id,
    ///   cckt.consume_keyword_type,
    ///   sd.room_seq,
    ///   u.user_id,
    ///   upm.card_alias,
    ///   ag.agg_group_seq
    /// FROM SPENT_DETAIL sd
    /// INNER JOIN COMMON_CONSUME_KEYWORD_TYPE cckt
    ///   ON sd.consume_keyword_type_id = cckt.consume_keyword_type_id
    /// INNER JOIN USERS u
    ///   ON sd.user_seq = u.user_seq
    /// INNER JOIN TELEGRAM_ROOM tr
    ///   ON sd.room_seq = tr.room_seq
    /// INNER JOIN USER_PAYMENT_METHODS upm
    ///   ON sd.payment_method_id = upm.payment_method_id
    /// INNER JOIN AGG_GROUP ag
    ///   ON tr.agg_group_seq = ag.agg_group_seq
    /// WHERE sd.should_index = 1
    ///   AND sd.spent_idx IN (:spent_idxs)
    ///   AND tr.is_room_approved = TRUE
    ///   AND ag.is_active = TRUE;
    /// ```
    ///
    /// Returns an empty vector without querying the database when `spent_idxs` is empty.
    async fn find_spent_details_for_indexing(
        &self,
        spent_idxs: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// Fetches `SPENT_DETAIL_INDEXING` rows in batches for full indexing.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   spent_idx,
    ///   spent_name,
    ///   spent_money,
    ///   spent_at,
    ///   created_at,
    ///   user_seq,
    ///   consume_keyword_type_id,
    ///   consume_keyword_type,
    ///   room_seq,
    ///   user_id,
    ///   card_alias,
    ///   updated_at,
    ///   updated_by,
    ///   agg_group_seq
    /// FROM SPENT_DETAIL_INDEXING
    /// ORDER BY spent_idx ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_spent_detail_indexing_for_index(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailIndexing>>;

    /// Fetches `SPENT_DETAIL_INDEXING` rows matching the provided spent IDs.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   spent_idx,
    ///   spent_name,
    ///   spent_money,
    ///   spent_at,
    ///   created_at,
    ///   user_seq,
    ///   consume_keyword_type_id,
    ///   consume_keyword_type,
    ///   room_seq,
    ///   user_id,
    ///   card_alias,
    ///   updated_at,
    ///   updated_by,
    ///   agg_group_seq
    /// FROM SPENT_DETAIL_INDEXING
    /// WHERE spent_idx IN (:ids);
    /// ```
    ///
    /// Returns an empty vector without querying the database when `ids` is empty.
    async fn find_spent_detail_indexing_by_ids(
        &self,
        ids: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailIndexing>>;

    /// Fetches raw spent-detail rows in batches without joining related tables.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   spent_idx,
    ///   spent_name,
    ///   spent_money,
    ///   spent_at,
    ///   should_index,
    ///   user_seq,
    ///   spent_group_id,
    ///   consume_keyword_type_id,
    ///   room_seq,
    ///   payment_method_id
    /// FROM SPENT_DETAIL
    /// ORDER BY spent_idx ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_spent_details(&self, offset: u64, limit: u64)
    -> anyhow::Result<Vec<SpentDetail>>;

    /// Updates consume type IDs for spent-detail rows in batches.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = CASE
    ///   WHEN spent_idx = :spent_idx_1 THEN :consume_keyword_type_id_1
    ///   WHEN spent_idx = :spent_idx_2 THEN :consume_keyword_type_id_2
    /// END
    /// WHERE spent_idx IN (:spent_idxs);
    /// ```
    ///
    /// Splits `updates` into `batch_size` chunks and executes each chunk in a transaction.
    async fn modify_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// Updates consume type IDs and names for indexing rows in batches.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE SPENT_DETAIL_INDEXING
    /// SET
    ///   consume_keyword_type_id = CASE
    ///     WHEN spent_idx = :spent_idx_1 THEN :consume_keyword_type_id_1
    ///     WHEN spent_idx = :spent_idx_2 THEN :consume_keyword_type_id_2
    ///   END,
    ///   consume_keyword_type = CASE
    ///     WHEN spent_idx = :spent_idx_1 THEN :consume_keyword_type_1
    ///     WHEN spent_idx = :spent_idx_2 THEN :consume_keyword_type_2
    ///   END
    /// WHERE spent_idx IN (:spent_idxs);
    /// ```
    ///
    /// Splits `updates` into `batch_size` chunks and executes each chunk in a transaction.
    async fn modify_spent_detail_indexing_type_batch(
        &self,
        updates: Vec<(i64, i64, String)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// Updates consume type IDs for spent-detail rows one row at a time.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = :consume_keyword_type_id
    /// WHERE spent_idx = :spent_idx;
    /// ```
    ///
    /// Executes one update per item in `updates` within a transaction.
    async fn modify_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64>;

    /// Bulk-upserts `DIM_CALENDAR` rows.
    ///
    /// Requested query:
    /// ```sql
    /// INSERT INTO DIM_CALENDAR (
    ///   dt,
    ///   yyyy,
    ///   mm,
    ///   dd,
    ///   yyyymm,
    ///   yyyymmdd,
    ///   day_of_month,
    ///   quarter_no,
    ///   half_no,
    ///   weekday_no,
    ///   is_weekend,
    ///   is_weekday,
    ///   is_month_start,
    ///   is_month_end,
    ///   remaining_days_in_month,
    ///   is_holiday,
    ///   is_before_holiday,
    ///   is_after_holiday,
    ///   created_at,
    ///   updated_at,
    ///   created_by,
    ///   updated_by
    /// )
    /// VALUES (...)
    /// ON DUPLICATE KEY UPDATE
    ///   yyyy = VALUES(yyyy),
    ///   mm = VALUES(mm),
    ///   dd = VALUES(dd),
    ///   yyyymm = VALUES(yyyymm),
    ///   yyyymmdd = VALUES(yyyymmdd),
    ///   day_of_month = VALUES(day_of_month),
    ///   quarter_no = VALUES(quarter_no),
    ///   half_no = VALUES(half_no),
    ///   weekday_no = VALUES(weekday_no),
    ///   is_weekend = VALUES(is_weekend),
    ///   is_weekday = VALUES(is_weekday),
    ///   is_month_start = VALUES(is_month_start),
    ///   is_month_end = VALUES(is_month_end),
    ///   remaining_days_in_month = VALUES(remaining_days_in_month),
    ///   is_holiday = VALUES(is_holiday),
    ///   is_before_holiday = VALUES(is_before_holiday),
    ///   is_after_holiday = VALUES(is_after_holiday),
    ///   updated_at = :now,
    ///   updated_by = 'batch';
    /// ```
    ///
    /// Duplicate detection uses the primary key `dt`.
    async fn input_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()>;

    /// Upserts denormalized spent-detail rows for indexing.
    ///
    /// Requested query:
    /// ```sql
    /// INSERT INTO SPENT_DETAIL_INDEXING (
    ///   spent_idx,
    ///   spent_name,
    ///   spent_money,
    ///   spent_at,
    ///   created_at,
    ///   user_seq,
    ///   consume_keyword_type_id,
    ///   consume_keyword_type,
    ///   room_seq,
    ///   user_id,
    ///   card_alias,
    ///   updated_at,
    ///   updated_by,
    ///   agg_group_seq
    /// )
    /// VALUES (...)
    /// ON DUPLICATE KEY UPDATE
    ///   spent_name = VALUES(spent_name),
    ///   spent_money = VALUES(spent_money),
    ///   spent_at = VALUES(spent_at),
    ///   created_at = VALUES(created_at),
    ///   user_seq = VALUES(user_seq),
    ///   consume_keyword_type_id = VALUES(consume_keyword_type_id),
    ///   consume_keyword_type = VALUES(consume_keyword_type),
    ///   room_seq = VALUES(room_seq),
    ///   user_id = VALUES(user_id),
    ///   card_alias = VALUES(card_alias),
    ///   updated_at = VALUES(updated_at),
    ///   updated_by = VALUES(updated_by),
    ///   agg_group_seq = VALUES(agg_group_seq);
    /// ```
    ///
    /// Duplicate detection uses the primary key `spent_idx`.
    async fn modify_spent_detail_indexing(
        &self,
        upsert_list: Vec<SpentDetailWithRelations>,
    ) -> anyhow::Result<()>;

    /// Deletes indexing rows matching the provided spent IDs.
    ///
    /// Requested query:
    /// ```sql
    /// DELETE FROM SPENT_DETAIL_INDEXING
    /// WHERE spent_idx IN (:spent_idxs);
    /// ```
    ///
    /// Returns without querying the database when `delete_list` is empty.
    async fn delete_spent_detail_indexing(&self, delete_list: &[i64]) -> anyhow::Result<()>;

    /// Fetches active email recipient groups in batches.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   agg_group_seq,
    ///   email_id,
    ///   is_active,
    ///   created_at,
    ///   updated_at,
    ///   created_by,
    ///   updated_by
    /// FROM SEND_EMAIL_AGG_GROUP
    /// WHERE is_active = TRUE
    /// ORDER BY agg_group_seq ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_send_email_agg_group(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SendEmailAggGroup>>;

    /// Fetches active currency exchange rate snapshots.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   exchange_rate_snapshot_seq,
    ///   base_currency_code,
    ///   target_currency_code,
    ///   base_amount,
    ///   exchange_rate,
    ///   is_active,
    ///   created_at,
    ///   updated_at,
    ///   created_by,
    ///   updated_by
    /// FROM CURRENCY_EXCHANGE_RATE_SNAPSHOT
    /// WHERE is_active = TRUE;
    /// ```
    async fn find_currency_exchange_rate_snapshot(
        &self,
    ) -> anyhow::Result<Vec<CurrencyExchangeRateSnapshot>>;

    /// Bulk-updates exchange rate values for currency exchange rate snapshots.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE CURRENCY_EXCHANGE_RATE_SNAPSHOT
    /// SET
    ///   exchange_rate = :exchange_rate,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE exchange_rate_snapshot_seq = :exchange_rate_snapshot_seq;
    /// ```
    ///
    /// Executes one update per item in `snapshot_map` within a transaction.
    async fn modify_currency_exchange_rate_snapshot_bulk(
        &self,
        snapshot_map: &HashMap<i64, f64>,
    ) -> anyhow::Result<()>;

    /// Fetches stock master data in batches.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   stock_seq,
    ///   market_seq,
    ///   stock_name,
    ///   api_symbol,
    ///   stock_price,
    ///   created_at,
    ///   updated_at,
    ///   created_by,
    ///   updated_by
    /// FROM STOCK
    /// ORDER BY stock_seq ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_stock_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<Stock>>;

    /// Bulk-updates current stock prices.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE STOCK
    /// SET
    ///   stock_price = :stock_price,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE stock_seq = :stock_seq;
    /// ```
    ///
    /// Executes one update per item in `price_map` within a transaction.
    async fn modify_stock_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()>;

    /// Fetches crypto master data in batches.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   crypto_seq,
    ///   crypto_name,
    ///   crypto_price,
    ///   api_symbol,
    ///   currency_code,
    ///   created_at,
    ///   updated_at,
    ///   created_by,
    ///   updated_by
    /// FROM CRYPTO
    /// ORDER BY crypto_seq ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_crypto_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<Crypto>>;

    /// Bulk-updates current crypto prices.
    ///
    /// Requested query:
    /// ```sql
    /// UPDATE CRYPTO
    /// SET
    ///   crypto_price = :crypto_price,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE crypto_seq = :crypto_seq;
    /// ```
    ///
    /// Executes one update per item in `price_map` within a transaction.
    async fn modify_crypto_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()>;

    /// Fetches the full stock market and currency type list.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT *
    /// FROM STOCK_TYPE;
    /// ```
    async fn find_stock_types(&self) -> anyhow::Result<Vec<StockType>>;

    /// Fetches per-user total stock valuation for the provided currency and user list.
    ///
    /// Requested query:
    /// ```sql
    //  SELECT
    //    sa.user_seq,
    //    SUM(sa.stock_cnt * s.stock_price) AS asset_sum
    //  FROM STOCK_ASSET sa
    //  INNER JOIN STOCK s
    //    ON sa.stock_seq = s.stock_seq
    //  INNER JOIN STOCK_TYPE st
    //    ON s.market_seq = st.market_seq
    //  WHERE st.currency_code = :currency_code
    //    AND sa.user_seq IN (:user_seqs)
    //  GROUP BY sa.user_seq;
    /// ```
    ///
    /// Returns an empty vector without querying the database when `user_seqs` is empty.
    async fn find_stock_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>>;

    /// Fetches user primary keys in ascending order by batch.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT user_seq
    /// FROM USERS
    /// ORDER BY user_seq ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_user_seq_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<i64>>;

    /// Fetches per-user total crypto valuation for the provided currency and user list.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   ca.user_seq,
    ///   SUM(c.crypto_price * ca.crypto_cnt) AS asset_sum
    /// FROM CRYPTO_ASSET ca
    /// INNER JOIN CRYPTO c
    ///   ON ca.crypto_seq = c.crypto_seq
    /// INNER JOIN CURRENCY_CODE cc
    ///   ON cc.currency_code = c.currency_code
    /// WHERE c.currency_code = :currency_code
    ///   AND ca.user_seq IN (:user_seqs)
    /// GROUP BY ca.user_seq;
    /// ```
    async fn find_crypto_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>>;

    /// Bulk-inserts `USER_CURRENT_ASSET_SNAPSHOT` rows.
    ///
    /// Each call persists one snapshot per user for the current aggregation run.
    /// Rows are inserted as new historical records (no upsert).
    async fn input_user_current_asset_snapshot_bulk(
        &self,
        rows: Vec<crate::entity::user_current_asset_snapshot::ActiveModel>,
    ) -> anyhow::Result<()>;

    /// Fetches per-user total cash valuation for the provided currency and user list.
    ///
    /// Requested query:
    /// ```sql
    /// SELECT
    ///   ca.user_seq,
    ///   SUM(ca.cash) AS asset_sum
    /// FROM CASH_ASSET ca
    /// INNER JOIN CURRENCY_CODE cc
    ///   ON cc.currency_code = ca.currency_code
    /// WHERE ca.currency_code = :currency_code
    ///   AND ca.user_seq IN (:user_seqs)
    /// GROUP BY ca.user_seq;
    /// ```
    async fn find_cash_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<AssetAmount>>;
}
