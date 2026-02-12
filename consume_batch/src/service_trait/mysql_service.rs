use crate::common::*;
use crate::models::{SpentDetailWithRelations, SpentTypeKeyword};

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
    async fn fetch_spent_type_keywords_batch(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentTypeKeyword>>;

    /// Fetches spent details with related information for indexing.
    ///
    /// Queries the JOIN of SPENT_DETAIL, COMMON_CONSUME_PRODT_KEYWORD,
    /// COMMON_CONSUME_KEYWORD_TYPE, USERS, and TELEGRAM_ROOM tables.
    ///
    /// Retrieves only records where:
    /// - `should_index` is 1 (true)
    /// - `is_room_approved` is 1 (true)
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting row number for pagination
    /// * `limit` - The maximum number of rows to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of `SpentDetailWithRelations` instances on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - Query execution fails
    /// - Data cannot be mapped to the model
    async fn fetch_spent_details_for_indexing(
        &self,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<SpentDetailWithRelations>>;
}
