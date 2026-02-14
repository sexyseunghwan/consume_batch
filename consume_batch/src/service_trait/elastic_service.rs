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
    async fn update_write_alias(&self, write_alias: &str, target_index: &str) -> anyhow::Result<()>;

    /// Updates read alias with optional traffic weight for Blue/Green deployment.
    ///
    /// Enables gradual traffic shifting between old (Blue) and new (Green) indices:
    /// - weight = 0.0: All traffic to old index
    /// - weight = 0.5: 50/50 split
    /// - weight = 1.0: All traffic to new index
    ///
    /// # Arguments
    ///
    /// * `read_alias` - The read alias name (e.g., "read_spent_detail_dev")
    /// * `new_index` - The new index name
    /// * `traffic_weight` - Percentage of traffic to route to new index (0.0 ~ 1.0)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful alias update.
    async fn update_read_alias_with_weight(
        &self,
        read_alias: &str,
        new_index: &str,
        traffic_weight: f32,
    ) -> anyhow::Result<()>;

    async fn get_consume_type_judgement(
        &self,
        prodt_name: &str,
    ) -> Result<ConsumingIndexProdtType, anyhow::Error>;

    async fn get_query_result_vec<T: DeserializeOwned>(
        &self,
        response_body: &Value,
    ) -> Result<Vec<DocumentWithId<T>>, anyhow::Error>;
}
