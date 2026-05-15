use crate::common::*;
use crate::entity::spent_detail_indexing;
use crate::repository::mysql_repository::MysqlRepository;

use super::MysqlServiceImpl;

impl<R: MysqlRepository + Send + Sync> MysqlServiceImpl<R> {
    pub(super) async fn delete_spent_detail_indexing(
        &self,
        delete_list: &[i64],
    ) -> anyhow::Result<()> {
        if delete_list.is_empty() {
            return Ok(());
        }

        let db: &DatabaseConnection = self.db_conn.get_connection();

        let txn: DatabaseTransaction = db.begin().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::delete_spent_detail_indexing] Failed to begin transaction: {:#}",
                e
            );
        })?;

        let result: Result<sea_orm::DeleteResult, DbErr> =
            spent_detail_indexing::Entity::delete_many()
                .filter(spent_detail_indexing::Column::SpentIdx.is_in(delete_list.to_vec()))
                .exec(&txn)
                .await
                .inspect_err(|e| {
                    error!(
                        "[MysqlServiceImpl::delete_spent_detail_indexing] Delete failed: {:#}",
                        e
                    );
                });

        if let Err(e) = result {
            txn.rollback().await.inspect_err(|e| {
                error!(
                    "[MysqlServiceImpl::delete_spent_detail_indexing] Rollback failed: {:#}",
                    e
                );
            })?;
            return Err(e.into());
        }

        txn.commit().await.inspect_err(|e| {
            error!(
                "[MysqlServiceImpl::delete_spent_detail_indexing] Commit failed: {:#}",
                e
            );
        })?;

        Ok(())
    }
}
