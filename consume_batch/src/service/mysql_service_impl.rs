use crate::common::*;

use crate::service_trait::mysql_service::*;

use crate::entity::{
    common_consume_keyword_type, common_consume_prodt_keyword, spent_detail, telegram_room, users,
};
use crate::models::{SpentDetail, SpentDetailWithRelations, SpentTypeKeyword};

use sea_orm::{ColumnTrait, JoinType, QueryFilter, RelationTrait, sea_query::{CaseStatement, Expr}};

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
            .context(
                "[MysqlServiceImpl::fetch_spent_type_keywords_batch] Failed to execute query",
            )?;

        Ok(results)
    }

    /// Fetches spent details with related information for indexing.
    ///
    /// Executes the following SQL query:
    /// ```sql
    /// SELECT
    ///     sd.spent_idx,
    ///     sd.spent_name,
    ///     sd.spent_money,
    ///     sd.spent_at,
    ///     sd.created_at,
    ///     sd.user_seq,
    ///     cp.consume_keyword_id,
    ///     cp.consume_keyword,
    ///     ct.consume_keyword_type_id,
    ///     ct.consume_keyword_type,
    ///     t.room_seq
    /// FROM SPENT_DETAIL sd
    /// INNER JOIN COMMON_CONSUME_PRODT_KEYWORD cp
    ///     ON sd.consume_keyword_id = cp.consume_keyword_id
    /// INNER JOIN COMMON_CONSUME_KEYWORD_TYPE ct
    ///     ON cp.consume_keyword_type_id = ct.consume_keyword_type_id
    /// INNER JOIN USERS u
    ///     ON u.user_seq = sd.user_seq
    /// INNER JOIN TELEGRAM_ROOM t
    ///     ON u.user_seq = t.user_seq
    /// WHERE sd.should_index = 1
    ///     AND t.is_room_approved = 1
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
    /// Returns a vector of `SpentDetailWithRelations` instances.
    async fn fetch_spent_details_for_indexing(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        let results: Vec<SpentDetailWithRelations> = spent_detail::Entity::find()
            .join(
                JoinType::InnerJoin,
                spent_detail::Relation::CommonConsumeKeywordType.def(),
            )
            // JOIN with USERS
            .join(JoinType::InnerJoin, spent_detail::Relation::Users.def())
            // JOIN with TELEGRAM_ROOM
            .join(JoinType::InnerJoin, users::Relation::TelegramRoom.def())
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
            .column(telegram_room::Column::RoomSeq)
            // WHERE conditions
            .filter(spent_detail::Column::ShouldIndex.eq(1))
            .filter(telegram_room::Column::IsRoomApproved.eq(true))
            // Pagination
            .offset(offset)
            .limit(limit)
            .into_model::<SpentDetailWithRelations>()
            .all(db)
            .await
            .context(
                "[MysqlServiceImpl::fetch_spent_details_for_indexing] Failed to execute query",
            )?;

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
            .offset(offset)
            .limit(limit)
            .order_by_asc(spent_detail::Column::SpentIdx)
            .into_model::<SpentDetail>()
            .all(db)
            .await
            .context("[MysqlServiceImpl::fetch_spent_details] Failed to execute query")?;

        Ok(results)
    }
    
    async fn update_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64> {
        let db: &DatabaseConnection = self.db_conn.get_connection();

        if updates.is_empty() {
            return Ok(0);
        }

        let txn: DatabaseTransaction = db
            .begin()
            .await
            .context("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to begin transaction")?;

        const CHUNK_SIZE: usize = 500;
        let mut total_affected: u64 = 0;

        for chunk in updates.chunks(CHUNK_SIZE) {
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
                .col_expr(
                    spent_detail::Column::ConsumeKeywordTypeId,
                    case_stmt.into(),
                )
                .filter(spent_detail::Column::SpentIdx.is_in(ids))
                .exec(&txn)
                .await
                .context(
                    "[MysqlServiceImpl::update_spent_detail_type_batch] Failed to bulk update, rolling back",
                )?;

            total_affected += result.rows_affected;
        }

        txn.commit()
            .await
            .context("[MysqlServiceImpl::update_spent_detail_type_batch] Failed to commit transaction")?;

        Ok(total_affected)
    }
}
