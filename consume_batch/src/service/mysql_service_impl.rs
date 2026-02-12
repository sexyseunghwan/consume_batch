use crate::common::*;

use crate::service_trait::mysql_service::*;

use crate::entity::{
    common_consume_keyword_type, common_consume_prodt_keyword, spent_detail, telegram_room, users,
};
use crate::models::{SpentDetailWithRelations, SpentTypeKeyword};
use crate::repository::mysql_repository::*;
use sea_orm::{ColumnTrait, JoinType, QueryFilter, RelationTrait};

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
            // JOIN with COMMON_CONSUME_PRODT_KEYWORD
            .join(
                JoinType::InnerJoin,
                spent_detail::Relation::CommonConsumeProdtKeyword.def(),
            )
            // JOIN with COMMON_CONSUME_KEYWORD_TYPE
            .join(
                JoinType::InnerJoin,
                common_consume_prodt_keyword::Relation::CommonConsumeKeywordType.def(),
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
            .column(common_consume_prodt_keyword::Column::ConsumeKeywordId)
            .column(common_consume_prodt_keyword::Column::ConsumeKeyword)
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
}
