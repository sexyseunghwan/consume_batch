#![allow(dead_code)]
use rust_decimal::Decimal;

use crate::common::*;
use crate::entity::dim_calendar;
use crate::models::{
    Crypto, CurrencyExchangeRateSnapshot, SendEmailAggGroup, SpentDetail, SpentDetailIndexing,
    SpentDetailWithRelations, SpentTypeKeyword, Stock, StockAssetAmount, StockType,
};

/// MySQL 접근 계층에서 사용하는 데이터 조회/변경 계약.
///
/// 각 함수의 `요청 쿼리`는 실제 구현(`mysql_service_impl`)의 SeaORM 빌더를
/// 사람이 읽기 쉬운 SQL 형태로 요약한 것이다.
#[async_trait]
pub trait MysqlService {
    /// 소비 키워드와 소비 유형을 배치 단위로 조회한다.
    ///
    /// 요청 쿼리:
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

    // Deprecated: 기존 offset/limit 기반 spent detail 색인 조회 함수.
    // 현재는 spent_idx 목록을 받는 find_spent_details_for_indexing 함수로 대체됨.
    // async fn find_spent_details_for_indexing(
    //     &self,
    //     offset: u64,
    //     limit: u64,
    // ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// 지정한 지출 ID 목록에 대해 색인용 역정규화 데이터를 조회한다.
    ///
    /// 요청 쿼리:
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
    /// `spent_idxs`가 비어 있으면 DB 요청 없이 빈 벡터를 반환한다.
    async fn find_spent_details_for_indexing(
        &self,
        spent_idxs: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// 전체 색인용으로 `SPENT_DETAIL_INDEXING` 행을 배치 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 지정한 지출 ID 목록에 해당하는 `SPENT_DETAIL_INDEXING` 행을 조회한다.
    ///
    /// 요청 쿼리:
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
    /// `ids`가 비어 있으면 DB 요청 없이 빈 벡터를 반환한다.
    async fn find_spent_detail_indexing_by_ids(
        &self,
        ids: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailIndexing>>;

    /// 관련 테이블 조인 없이 원본 지출 상세 행을 배치 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 지출 상세의 소비 유형 ID를 배치 업데이트한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = CASE
    ///   WHEN spent_idx = :spent_idx_1 THEN :consume_keyword_type_id_1
    ///   WHEN spent_idx = :spent_idx_2 THEN :consume_keyword_type_id_2
    /// END
    /// WHERE spent_idx IN (:spent_idxs);
    /// ```
    ///
    /// `updates`를 `batch_size` 단위로 나눠 트랜잭션 안에서 반복 실행한다.
    async fn modify_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// 색인 테이블의 소비 유형 ID와 소비 유형명을 배치 업데이트한다.
    ///
    /// 요청 쿼리:
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
    /// `updates`를 `batch_size` 단위로 나눠 트랜잭션 안에서 반복 실행한다.
    async fn modify_spent_detail_indexing_type_batch(
        &self,
        updates: Vec<(i64, i64, String)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// 지출 상세의 소비 유형 ID를 한 행씩 업데이트한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = :consume_keyword_type_id
    /// WHERE spent_idx = :spent_idx;
    /// ```
    ///
    /// `updates`의 각 항목마다 트랜잭션 안에서 반복 실행한다.
    async fn modify_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64>;

    /// `DIM_CALENDAR` 행을 벌크 업서트한다.
    ///
    /// 요청 쿼리:
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
    /// 중복 기준은 기본키 `dt`다.
    async fn input_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()>;

    /// 색인용 지출 상세 역정규화 행을 업서트한다.
    ///
    /// 요청 쿼리:
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
    /// 중복 기준은 기본키 `spent_idx`다.
    async fn modify_spent_detail_indexing(
        &self,
        upsert_list: Vec<SpentDetailWithRelations>,
    ) -> anyhow::Result<()>;

    /// 지정한 지출 ID 목록에 해당하는 색인 행을 삭제한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// DELETE FROM SPENT_DETAIL_INDEXING
    /// WHERE spent_idx IN (:spent_idxs);
    /// ```
    ///
    /// `delete_list`가 비어 있으면 DB 요청 없이 종료한다.
    async fn delete_spent_detail_indexing(&self, delete_list: &[i64]) -> anyhow::Result<()>;

    /// 활성화된 이메일 발송 대상 그룹을 배치 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 활성화된 환율 스냅샷을 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 환율 스냅샷의 환율 값을 벌크 업데이트한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// UPDATE CURRENCY_EXCHANGE_RATE_SNAPSHOT
    /// SET
    ///   exchange_rate = :exchange_rate,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE exchange_rate_snapshot_seq = :exchange_rate_snapshot_seq;
    /// ```
    ///
    /// `snapshot_map`의 각 항목마다 트랜잭션 안에서 반복 실행한다.
    async fn modify_currency_exchange_rate_snapshot_bulk(
        &self,
        snapshot_map: &HashMap<i64, f64>,
    ) -> anyhow::Result<()>;

    /// 주식 마스터 정보를 배치 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 주식 현재가를 벌크 업데이트한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// UPDATE STOCK
    /// SET
    ///   stock_price = :stock_price,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE stock_seq = :stock_seq;
    /// ```
    ///
    /// `price_map`의 각 항목마다 트랜잭션 안에서 반복 실행한다.
    async fn modify_stock_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()>;

    /// 암호화폐 마스터 정보를 배치 조회한다.
    ///
    /// 요청 쿼리:
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

    /// 암호화폐 현재가를 벌크 업데이트한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// UPDATE CRYPTO
    /// SET
    ///   crypto_price = :crypto_price,
    ///   updated_at = :now,
    ///   updated_by = 'batch'
    /// WHERE crypto_seq = :crypto_seq;
    /// ```
    ///
    /// `price_map`의 각 항목마다 트랜잭션 안에서 반복 실행한다.
    async fn modify_crypto_price_bulk(
        &self,
        price_map: &HashMap<i64, Decimal>,
    ) -> anyhow::Result<()>;

    /// 주식 시장/통화 유형 목록을 전체 조회한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// SELECT *
    /// FROM STOCK_TYPE;
    /// ```
    async fn find_stock_types(&self) -> anyhow::Result<Vec<StockType>>;

    /// 지정한 통화와 사용자 목록에 대해 사용자별 주식 평가 금액 합계를 조회한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// SELECT
    ///   sa.user_seq,
    ///   SUM(sa.stock_cnt * s.stock_price) AS stock_sum
    /// FROM STOCK_ASSET sa
    /// INNER JOIN STOCK s
    ///   ON sa.stock_seq = s.stock_seq
    /// INNER JOIN STOCK_TYPE st
    ///   ON s.market_seq = st.market_seq
    /// WHERE st.currency_code = :currency_code
    ///   AND sa.user_seq IN (:user_seqs)
    /// GROUP BY sa.user_seq;
    /// ```
    ///
    /// `user_seqs`가 비어 있으면 DB 요청 없이 빈 벡터를 반환한다.
    async fn find_stock_asset_amount_batch(
        &self,
        currency_code: &str,
        user_seqs: &[i64],
    ) -> anyhow::Result<Vec<StockAssetAmount>>;

    /// 사용자 기본키를 오름차순으로 배치 조회한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// SELECT user_seq
    /// FROM USERS
    /// ORDER BY user_seq ASC
    /// LIMIT :limit OFFSET :offset;
    /// ```
    async fn find_user_seq_batch(&self, offset: u64, limit: u64) -> anyhow::Result<Vec<i64>>;

    /// 전체 사용자 수를 조회한다.
    ///
    /// 요청 쿼리:
    /// ```sql
    /// SELECT COUNT(*)
    /// FROM USERS;
    /// ```
    async fn find_users_size(&self) -> anyhow::Result<u64>;
}
