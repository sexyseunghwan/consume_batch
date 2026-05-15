use crate::common::*;
use crate::entity::{dim_calendar, spent_detail_indexing};
use crate::models::SpentDetailWithRelations;
use crate::repository::mysql_repository::MysqlRepository;
use sea_orm::sea_query::OnConflict;

use super::MysqlServiceImpl;

impl<R: MysqlRepository + Send + Sync> MysqlServiceImpl<R> {
    pub(super) async fn input_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();
        let now_dt: chrono::NaiveDateTime = Utc::now().naive_utc();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::input_dim_calendar_bulk] Failed to begin transaction: {:#}",
                e
            );
        })?;

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
                "[MysqlServiceImpl::input_dim_calendar_bulk] Bulk insert failed, rolling back: {:#}",
                e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::input_dim_calendar_bulk] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e.into());
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::input_dim_calendar_bulk] Commit failed: {:#}",
                e
            );
        })?;

        Ok(())
    }

    pub(super) async fn modify_spent_detail_indexing(
        &self,
        upsert_list: Vec<SpentDetailWithRelations>,
    ) -> anyhow::Result<()> {
        if upsert_list.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();
        let now: DateTime<Utc> = Utc::now();
        let spent_idxs: Vec<i64> = upsert_list.iter().map(|r| r.spent_idx).collect();

        let active_models: Vec<spent_detail_indexing::ActiveModel> = upsert_list
            .into_iter()
            .map(|row| spent_detail_indexing::ActiveModel {
                spent_idx: Set(row.spent_idx),
                spent_name: Set(row.spent_name),
                spent_money: Set(row.spent_money),
                spent_at: Set(row.spent_at.naive_utc()),
                created_at: Set(row.created_at.naive_utc()),
                user_seq: Set(row.user_seq),
                consume_keyword_type_id: Set(row.consume_keyword_type_id),
                consume_keyword_type: Set(row.consume_keyword_type),
                room_seq: Set(row.room_seq),
                user_id: Set(row.user_id),
                card_alias: Set(row.card_alias),
                updated_at: Set(Some(now.naive_utc())),
                updated_by: Set(Some("batch".to_string())),
                agg_group_seq: Set(row.agg_group_seq.unwrap_or(0)),
            })
            .collect();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let result: std::result::Result<u64, DbErr> =
            spent_detail_indexing::Entity::insert_many(active_models)
                .on_conflict(
                    OnConflict::column(spent_detail_indexing::Column::SpentIdx)
                        .update_columns([
                            spent_detail_indexing::Column::SpentName,
                            spent_detail_indexing::Column::SpentMoney,
                            spent_detail_indexing::Column::SpentAt,
                            spent_detail_indexing::Column::CreatedAt,
                            spent_detail_indexing::Column::UserSeq,
                            spent_detail_indexing::Column::ConsumeKeywordTypeId,
                            spent_detail_indexing::Column::ConsumeKeywordType,
                            spent_detail_indexing::Column::RoomSeq,
                            spent_detail_indexing::Column::UserId,
                            spent_detail_indexing::Column::CardAlias,
                            spent_detail_indexing::Column::UpdatedAt,
                            spent_detail_indexing::Column::UpdatedBy,
                            spent_detail_indexing::Column::AggGroupSeq,
                        ])
                        .to_owned(),
                )
                .exec_without_returning(&txn)
                .await;

        if let Err(e) = result {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing] Upsert failed (spent_idxs={:?}): {:#}",
                spent_idxs, e
            );
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::modify_spent_detail_indexing] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e.into());
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::modify_spent_detail_indexing] Commit failed (spent_idxs={:?}): {:#}",
                spent_idxs, e
            );
        })?;

        Ok(())
    }
}
