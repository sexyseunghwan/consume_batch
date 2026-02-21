use crate::common::*;

use crate::models::{ConsumingIndexProdtType, DocumentWithId};

#[async_trait]
pub trait ElasticService {
    /// Creates a new Elasticsearch index with specified settings and mappings.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index to create
    /// * `settings` - Index settings (number of shards, replicas, refresh_interval, etc.)
    /// * `mappings` - Field mappings schema
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful index creation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Index already exists
    /// - Invalid settings or mappings
    /// - Network/connection failure
    async fn create_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()>;

    /// Updates index settings (e.g., number of replicas, refresh_interval).
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index to update
    /// * `settings` - New settings to apply
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful update.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Index does not exist
    /// - Invalid settings
    /// - Network/connection failure
    async fn update_index_settings(&self, index_name: &str, settings: &Value)
    -> anyhow::Result<()>;

    /// Performs bulk indexing of documents.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Any type that implements `Serialize`
    ///
    /// # Arguments
    ///
    /// * `index_name` - The target index name
    /// * `documents` - Vector of documents to index
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful bulk indexing.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any document fails to index
    /// - Mapping conflicts occur
    /// - Network/connection failure
    async fn bulk_index<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> anyhow::Result<()>;

    /// Performs bulk update of documents.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Any type that implements `Serialize`
    ///
    /// # Arguments
    ///
    /// * `index_name` - The target index name
    /// * `documents` - Vector of documents to update
    /// * `doc_id_field` - The field name to use as document ID (e.g., "spent_idx", "id")
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful bulk update.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any document fails to update
    /// - Network/connection failure
    async fn bulk_update<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: &str,
    ) -> anyhow::Result<()>;

    /// Performs bulk delete of documents by IDs.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The target index name
    /// * `doc_ids` - Vector of document IDs to delete
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful bulk delete.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any document fails to delete
    /// - Network/connection failure
    async fn bulk_delete(&self, index_name: &str, doc_ids: Vec<i64>) -> anyhow::Result<()>;

    /// Swaps index alias from old index to new index atomically.
    ///
    /// This operation:
    /// 1. Removes the alias from the old index (if it exists)
    /// 2. Adds the alias to the new index
    ///
    /// Both operations are performed atomically.
    ///
    /// # Arguments
    ///
    /// * `alias_name` - The alias name
    /// * `new_index_name` - The new index to point the alias to
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful alias swap.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - New index does not exist
    /// - Alias update fails
    /// - Network/connection failure
    async fn swap_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()>;

    /// Updates write alias to point to a specific index.
    ///
    /// This is used for Blue/Green deployment to control which index receives writes.
    /// Unlike read aliases (which can be split for traffic testing), write aliases
    /// should point to a single index at a time.
    ///
    /// # Arguments
    ///
    /// * `write_alias` - The write alias name (e.g., "write_spent_detail_dev")
    /// * `target_index` - The index to point the write alias to
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful alias update.
    async fn update_write_alias(&self, write_alias: &str, target_index: &str)
    -> anyhow::Result<()>;

    /// Updates read alias.
    ///
    /// # Arguments
    ///
    /// * `read_alias` - The read alias name (e.g., "read_spent_detail_dev")
    /// * `new_index` - The new index name
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful alias update.
    async fn update_read_alias(&self, read_alias: &str, new_index: &str) -> anyhow::Result<()>;

    /// Finalizes a full index after bulk indexing is complete.
    ///
    /// 1. Updates index settings to production values (replicas=1, refresh=1s)
    /// 2. Swaps alias from old index to the new index
    async fn finalize_full_index(
        &self,
        index_alias: &str,
        new_index_name: &str,
    ) -> anyhow::Result<Vec<String>>;

    /// Prepares a new Elasticsearch index for full indexing.
    ///
    /// 1. Reads the mapping schema from `mapping_schema_path`
    /// 2. Merges bulk indexing settings (shards=3, replicas=0, refresh=-1)
    /// 3. Creates a new timestamped index (e.g. `{index_alias}_20260213120000`)
    ///
    /// Returns the created index name.
    async fn prepare_full_index(
        &self,
        index_name: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String>;

    /// Deletes multiple Elasticsearch indices.
    ///
    /// Intended for cleanup after a Blue/Green index swap — the old index
    /// that is no longer aliased can be removed by passing its name here.
    /// Silently succeeds if the provided slice is empty.
    ///
    /// # Arguments
    ///
    /// * `index_names` - Slice of index names to delete
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all indices were deleted successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if any deletion fails (e.g. index does not exist,
    /// network failure, or insufficient permissions).
    async fn delete_indices(&self, index_names: &[String]) -> anyhow::Result<()>;

    async fn get_consume_type_judgement(
        &self,
        prodt_name: &str,
    ) -> Result<ConsumingIndexProdtType, anyhow::Error>;

    async fn get_query_result_vec<T: DeserializeOwned>(
        &self,
        response_body: &Value,
    ) -> Result<Vec<DocumentWithId<T>>, anyhow::Error>;
}
