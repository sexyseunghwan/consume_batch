use crate::common::*;

use crate::service_trait::mysql_service::*;

use crate::entity::{
    common_consume_keyword_type, common_consume_prodt_keyword, dim_calendar, spent_detail,
    telegram_room, users,
};
use crate::models::{SpentDetail, SpentDetailWithRelations, SpentTypeKeyword};

use sea_orm::{
    ColumnTrait, JoinType, QueryFilter, QuerySelect, RelationTrait,
    sea_query::{CaseStatement, Expr, OnConflict},
};

use crate::repository::mysql_repository::*;

#[derive(Debug, Getters, Clone, new)]
pub struct MysqlServiceImpl<R: MysqlRepository> {
    db_conn: R,
}

#[async_trait]
impl<R> MysqlService for MysqlServiceImpl<R>
where
    R: MysqlRepository + Send + Sync,
{
    /// Fetches keyword type data in batches using a JOIN query.
    ///
    /// Executes the following SQL query:
    /// ```sql
    /// SELECT
    ///     c.consume_keyword_type,
    ///     p.consume_keyword,
    ///     p.keyword_weight
    /// FROM COMMON_CONSUME_KEYWORD_TYPE c
    /// INNER JOIN COMMON_CONSUME_PRODT_KEYWORD p
    ///     ON c.consume_keyword_type_id = p.consume_keyword_type_id
    /// LIMIT {limit} OFFSET {offset}
    /// ```
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting row for pagination
    /// * `limit` - The maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SpentTypeKeyword` instances.
    async fn fetch_spent_type_keywords_batch(
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
                error!("[MysqlServiceImpl::fetch_spent_type_keywords_batch] Failed to execute query: {:#}", e);
            })?;

        Ok(results)
    }

    /// Fetches spent details with related information for indexing.
    ///
    /// Executes the following SQL query:
    /// ```sql
    // SELECT
    // 	sd.spent_idx,
    // 	sd.spent_name,
    // 	sd.spent_money,
    // 	sd.spent_at,
    // 	sd.created_at,
    // 	sd.user_seq,
    // 	ct.consume_keyword_type_id,
    // 	ct.consume_keyword_type,
    // 	t.room_seq,
    //     u.user_id
    // 	FROM SPENT_DETAIL sd
    // 	INNER JOIN COMMON_CONSUME_KEYWORD_TYPE ct ON sd.consume_keyword_type_id = ct.consume_keyword_type_id
    // 	INNER JOIN USERS u ON u.user_seq = sd.user_seq
    // 	INNER JOIN TELEGRAM_ROOM t ON t.room_seq = sd.room_seq
    // WHERE sd.should_index = 1
    // AND	t.is_room_approved = true;
    /// ```
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting row for pagination
    /// * `limit` - The maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SpentDetailWithRelations` instances.
    async fn fetch_spent_details_for_indexing(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();
        let produced_at: DateTime<Utc> = Utc::now();

        let results: Vec<SpentDetailWithRelations> = spent_detail::Entity::find()
            .join(
                JoinType::InnerJoin,
                spent_detail::Relation::CommonConsumeKeywordType.def(),
            )
            // JOIN with USERS
            .join(JoinType::InnerJoin, spent_detail::Relation::Users.def())
            // JOIN with TELEGRAM_ROOM
            .join(JoinType::InnerJoin, spent_detail::Relation::TelegramRoom.def())
            // SELECT specific columns
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
            // Add literal value for indexing_type
            .expr_as(Expr::value("I"), "indexing_type")
            // Add current timestamp for produced_at
            .expr_as(Expr::value(produced_at), "produced_at")
            .column(users::Column::UserId)
            // WHERE conditions
            .filter(spent_detail::Column::ShouldIndex.eq(1))
            .filter(telegram_room::Column::IsRoomApproved.eq(true))
            // Pagination
            .offset(offset)
            .limit(limit)
            .into_model::<SpentDetailWithRelations>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!("[MysqlServiceImpl::fetch_spent_details_for_indexing] Failed to execute query: {:#}", e);
            })?;

        Ok(results)
    }

    async fn fetch_spent_details(
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
            .offset(offset)
            .limit(limit)
            .order_by_asc(spent_detail::Column::SpentIdx)
            .into_model::<SpentDetail>()
            .all(db)
            .await
            .inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::fetch_spent_details] Failed to execute query: {:#}",
                    e
                );
            })?;

        Ok(results)
    }

    /// Bulk updates the `consume_keyword_type_id` column in the SPENT_DETAIL table.
    ///
    /// Splits the given `updates` list into chunks of `CHUNK_SIZE` (500 rows each)
    /// and executes a single CASE WHEN query per chunk.
    /// All chunks run within a single transaction;
    /// if any chunk fails, the entire transaction is explicitly rolled back.
    ///
    /// ## Generated SQL (once per chunk)
    ///
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = CASE
    ///     WHEN spent_idx = 1 THEN 10
    ///     WHEN spent_idx = 2 THEN 20
    ///     WHEN spent_idx = 3 THEN 30
    /// END
    /// WHERE spent_idx IN (1, 2, 3)
    /// ```
    ///
    /// ## Transaction flow
    ///
    /// ```text
    /// BEGIN
    ///   ├─ chunk[0] UPDATE ... (up to 500 rows)
    ///   ├─ chunk[1] UPDATE ... (up to 500 rows)
    ///   ├─ ...
    ///   ├─ On failure → ROLLBACK → return Err
    ///   └─ All success → COMMIT
    /// ```
    async fn update_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        if updates.is_empty() {
            return Ok(0);
        }

        // Wrap all chunks in a single transaction.
        let txn: DatabaseTransaction = db.begin().await
            .inspect_err(|e| {
                error!("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to begin transaction: {:#}", e);
            })?;

        /// Max number of CASE WHEN clauses per chunk.
        /// Limited to 500 to stay within MySQL's max_allowed_packet and reduce query parsing overhead.
        const CHUNK_SIZE: usize = 500;
        let mut total_affected: u64 = 0;

        // Wrapped in an async block so that any error is captured in `result`,
        // allowing explicit rollback in the branch below.
        let result: std::result::Result<(), anyhow::Error> = async {
            for chunk in updates.chunks(CHUNK_SIZE) {
                // Dynamically build CASE WHEN spent_idx = ? THEN ? ... END expression
                let mut case_stmt: CaseStatement = CaseStatement::new();
                // Collect primary keys for WHERE spent_idx IN (...) clause
                let mut ids: Vec<i64> = Vec::with_capacity(chunk.len());

                for (spent_idx, new_type_id) in chunk {
                    case_stmt = case_stmt.case(
                        Expr::col(spent_detail::Column::SpentIdx).eq(*spent_idx),
                        Expr::value(*new_type_id),
                    );
                    ids.push(*spent_idx);
                }

                // UPDATE SPENT_DETAIL
                // SET consume_keyword_type_id = CASE WHEN ... END
                // WHERE spent_idx IN (...)
                let result: sea_orm::UpdateResult = spent_detail::Entity::update_many()
                    // SET
                    .col_expr(spent_detail::Column::ConsumeKeywordTypeId, case_stmt.into())
                    // WHERE
                    .filter(spent_detail::Column::SpentIdx.is_in(ids))
                    .exec(&txn)
                    .await
                    .inspect_err(|e| {
                        error!("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to bulk update: {:#}", e);
                    })?;

                total_affected += result.rows_affected;
            }

            Ok::<(), anyhow::Error>(())
        }
        .await;

        // Explicit ROLLBACK on failure
        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::update_spent_detail_type_batch] Rolling back transaction: {}",
                e
            );
            txn.rollback().await
                .inspect_err(|e| {
                    error!("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to rollback transaction: {:#}", e);
                })?;
            return Err(e);
        }

        // COMMIT when all chunks succeed
        txn.commit().await
            .inspect_err(|e| {
                error!("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to commit transaction: {:#}", e);
            })?;

        Ok(total_affected)
    }

    /// Updates consume_keyword_type_id one row at a time.
    ///
    /// Executes individual UPDATE statements per row within a single transaction.
    /// For performance comparison with `update_spent_detail_type_batch`.
    ///
    /// ## Generated SQL (per row)
    ///
    /// ```sql
    /// UPDATE SPENT_DETAIL
    /// SET consume_keyword_type_id = ?
    /// WHERE spent_idx = ?
    /// ```
    async fn update_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        if updates.is_empty() {
            return Ok(0);
        }

        let txn: DatabaseTransaction = db.begin().await
            .inspect_err(|e| {
                error!("[MysqlServiceImpl::update_spent_detail_type_one_by_one] Failed to begin transaction: {:#}", e);
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
                        error!("[MysqlServiceImpl::update_spent_detail_type_one_by_one] Failed to update row: {:#}", e);
                    })?;

                total_affected += result.rows_affected;
            }

            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::update_spent_detail_type_one_by_one] Rolling back transaction: {}",
                e
            );
            txn.rollback()
                .await
                .inspect_err(|e| {
                    error!("[MysqlServiceImpl::update_spent_detail_type_one_by_one] Failed to rollback transaction: {:#}", e);
                })?;
            return Err(e);
        }

        txn.commit().await
            .inspect_err(|e| {
                error!("[MysqlServiceImpl::update_spent_detail_type_one_by_one] Failed to commit transaction: {:#}", e);
            })?;

        Ok(total_affected)
    }

    /// Bulk-inserts DIM_CALENDAR rows within a transaction.
    ///
    /// All rows are inserted atomically — if any insert fails the entire batch
    /// is rolled back. Duplicate `dt` PKs are fully overwritten with the new data.
    async fn insert_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::insert_dim_calendar_bulk] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let now_dt: chrono::NaiveDateTime = Utc::now().naive_utc();

        let result: std::result::Result<u64, DbErr> = dim_calendar::Entity::insert_many(rows)
            .on_conflict(
                OnConflict::column(dim_calendar::Column::Dt)
                    .update_columns([
                        dim_calendar::Column::Yyyy,
                        dim_calendar::Column::Mm,
                        dim_calendar::Column::Dd,
                        dim_calendar::Column::Yyyymm,
                        dim_calendar::Column::Yyyymmdd,
                        dim_calendar::Column::DayOfMonth,
                        dim_calendar::Column::QuarterNo,
                        dim_calendar::Column::HalfNo,
                        dim_calendar::Column::WeekdayNo,
                        dim_calendar::Column::IsWeekend,
                        dim_calendar::Column::IsWeekday,
                        dim_calendar::Column::IsMonthStart,
                        dim_calendar::Column::IsMonthEnd,
                        dim_calendar::Column::RemainingDaysInMonth,
                        dim_calendar::Column::IsHoliday,
                        dim_calendar::Column::IsBeforeHoliday,
                        dim_calendar::Column::IsAfterHoliday,
                    ])
                    .value(dim_calendar::Column::UpdatedAt, now_dt)
                    .value(dim_calendar::Column::UpdatedBy, "batch")
                    .to_owned(),
            )
            .exec_without_returning(&txn)
            .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::insert_dim_calendar_bulk] Bulk insert failed, rolling back: {:#}",
                e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::insert_dim_calendar_bulk] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e.into());
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::insert_dim_calendar_bulk] Commit failed: {:#}",
                e
            );
        })?;

        Ok(())
    }
}
