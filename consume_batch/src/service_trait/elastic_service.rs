use crate::common::*;

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
}
