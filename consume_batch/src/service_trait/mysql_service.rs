#![allow(dead_code)]
use crate::common::*;
use crate::entity::dim_calendar;
use crate::models::{
    SendEmailAggGroup, SpentDetail, SpentDetailIndexing, SpentDetailWithRelations, SpentTypeKeyword,
};

#[async_trait]
pub trait MysqlService {
    /// Fetches keyword type data in batches from the database.
    ///
    /// Queries the JOIN of COMMON_CONSUME_KEYWORD_TYPE and COMMON_CONSUME_PRODT_KEYWORD
    /// tables, retrieving data in batches for efficient processing.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting row number for pagination
    /// * `limit` - The maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SpentTypeKeyword` instances on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - Query execution fails
    /// - Data cannot be mapped to the model
    async fn find_spent_type_keywords_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentTypeKeyword>>;
    // Fetches spent details with related information for indexing. (deprecated — replaced by spent_idxs variant)
    // async fn find_spent_details_for_indexing(
    //     &self,
    //     offset: u64,
    //     limit: u64,
    // ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// Fetches spent details with related data for selected spent IDs.
    ///
    /// Retrieves denormalized spend detail rows used to update
    /// `SPENT_DETAIL_INDEXING` during incremental indexing.
    ///
    /// # Arguments
    ///
    /// * `spent_idxs` - Spend detail primary keys to fetch
    ///
    /// # Returns
    ///
    /// Returns the matching `SpentDetailWithRelations` rows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database query execution fails
    /// - Query results cannot be mapped to the model
    async fn find_spent_details_for_indexing(
        &self,
        spent_idxs: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;

    /// Fetches all spent detail indexing rows in batches.
    ///
    /// Reads `SPENT_DETAIL_INDEXING` records using offset-based pagination for
    /// full-indexing jobs.
    ///
    /// # Arguments
    ///
    /// * `offset` - Starting row offset for pagination
    /// * `limit` - Maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SpentDetailIndexing` rows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database query execution fails
    /// - Query results cannot be mapped to the model
    async fn find_spent_detail_indexing_for_index(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailIndexing>>;

    /// 증분 색인용: 주어진 `spent_idx` 목록에 해당하는 역정규화 행을 조회한다.
    ///
    /// `WHERE spent_idx IN (...)` 쿼리로 동작하며, 존재하지 않는 ID는 결과에서 자동으로 제외된다.
    async fn find_spent_detail_indexing_by_ids(
        &self,
        ids: &[i64],
    ) -> anyhow::Result<Vec<SpentDetailIndexing>>;

    /// Fetches raw spent-detail rows without joining related tables.
    async fn find_spent_details(&self, offset: u64, limit: u64)
    -> anyhow::Result<Vec<SpentDetail>>;

    /// Updates consume_keyword_type_id for multiple spent_detail records.
    ///
    /// # Arguments
    ///
    /// * `updates` - Vec of (spent_idx, new_consume_keyword_type_id)
    ///
    /// # Returns
    ///
    /// Returns the number of updated rows.
    async fn modify_spent_detail_type_batch(
        &self,
        updates: Vec<(i64, i64)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// Updates consume_keyword_type_id and consume_keyword_type for multiple spent_detail_indexing records.
    ///
    /// # Arguments
    ///
    /// * `updates` - Vec of (spent_idx, new_consume_keyword_type_id, new_consume_keyword_type)
    /// * `batch_size` - Number of rows to process per SQL statement
    ///
    /// # Returns
    ///
    /// Returns the number of updated rows.
    async fn modify_spent_detail_indexing_type_batch(
        &self,
        updates: Vec<(i64, i64, String)>,
        batch_size: usize,
    ) -> anyhow::Result<u64>;

    /// Updates consume_keyword_type_id one row at a time (for performance comparison).
    async fn modify_spent_detail_type_one_by_one(
        &self,
        updates: Vec<(i64, i64)>,
    ) -> anyhow::Result<u64>;

    /// Bulk-inserts DIM_CALENDAR rows, ignoring duplicate PKs (dt).
    async fn input_dim_calendar_bulk(
        &self,
        rows: Vec<dim_calendar::ActiveModel>,
    ) -> anyhow::Result<()>;

    /// Upserts denormalized spent detail indexing rows.
    ///
    /// Inserts or updates `SPENT_DETAIL_INDEXING` rows from the provided related
    /// spent detail data.
    ///
    /// # Arguments
    ///
    /// * `upsert_list` - Denormalized spent detail rows to insert or update
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after all rows are written.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database transaction creation fails
    /// - Bulk upsert execution fails
    /// - Transaction commit fails
    async fn modify_spent_detail_indexing(
        &self,
        upsert_list: Vec<SpentDetailWithRelations>,
    ) -> anyhow::Result<()>;

    /// Deletes denormalized spent detail indexing rows by spent ID.
    ///
    /// Removes rows from `SPENT_DETAIL_INDEXING` that match the provided
    /// `spent_idx` values.
    ///
    /// # Arguments
    ///
    /// * `delete_list` - Spend detail primary keys to delete
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the delete operation completes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database delete execution fails
    async fn delete_spent_detail_indexing(&self, delete_list: &[i64]) -> anyhow::Result<()>;

    /// Fetches active email recipients grouped by aggregate group.
    ///
    /// Reads recipient rows used by the monthly spend report job, with
    /// offset-based pagination.
    ///
    /// # Arguments
    ///
    /// * `offset` - Starting row offset for pagination
    /// * `limit` - Maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SendEmailAggGroup` recipient mappings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database query execution fails
    /// - Query results cannot be mapped to the model
    async fn find_send_email_agg_group(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SendEmailAggGroup>>;
}
