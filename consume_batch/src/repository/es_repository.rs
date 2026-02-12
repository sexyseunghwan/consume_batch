//! Elasticsearch repository implementation.
//!
//! This module provides the data access layer for Elasticsearch operations,
//! including document search, indexing, and deletion.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      EsRepositoryImpl                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌─────────────────────────────────────────────────────────┐    │
//! │  │              Elasticsearch Client                       │    │
//! │  │         (Multi-node Connection Pool)                    │    │
//! │  └─────────────────────────────────────────────────────────┘    │
//! │                            │                                    │
//! │         ┌──────────────────┼──────────────────┐                 │
//! │         ▼                  ▼                  ▼                 │
//! │  ┌────────────┐    ┌────────────┐    ┌────────────┐             │
//! │  │   Search   │    │   Index    │    │   Delete   │             │
//! │  │   Query    │    │  Document  │    │  Document  │             │
//! │  └────────────┘    └────────────┘    └────────────┘             │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Connection Management
//!
//! The repository uses a round-robin multi-node connection pool for:
//! - Load balancing across multiple Elasticsearch nodes
//! - Automatic failover when a node becomes unavailable
//! - Configurable timeout (30 seconds default)
//!
//! # Environment Variables
//!
//! | Variable   | Description                              | Example                          |
//! |------------|------------------------------------------|----------------------------------|
//! | `ES_DB_URL`| Comma-separated list of ES hosts         | `host1:9200,host2:9200`          |
//! | `ES_ID`    | Elasticsearch username (optional)        | `elastic`                        |
//! | `ES_PW`    | Elasticsearch password (optional)        | `password`                       |

use crate::{app_config::AppConfig, common::*};

use elasticsearch::{
    BulkParts,
    http::request::JsonBody,
    indices::{
        IndicesCreateParts, IndicesGetAliasParts, IndicesPutSettingsParts,
        IndicesUpdateAliasesParts,
    },
};
/// Trait defining Elasticsearch repository operations.
///
/// This trait abstracts the Elasticsearch data access layer, allowing for
/// different implementations (e.g., mock implementations for testing).
///
/// # Implementors
///
/// - [`EsRepositoryImpl`] - Production implementation with real ES client
#[async_trait]
pub trait EsRepository {
    /// Executes a search query against an Elasticsearch index.
    ///
    /// # Arguments
    ///
    /// * `es_query` - The Elasticsearch query DSL as a JSON value
    /// * `index_name` - The target index name to search
    ///
    /// # Returns
    ///
    /// Returns `Ok(Value)` containing the search response on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The Elasticsearch cluster returns an error response
    /// - The response cannot be parsed as JSON
    async fn get_search_query(
        &self,
        es_query: &Value,
        index_name: &str,
    ) -> Result<Value, anyhow::Error>;

    /// Indexes a document into an Elasticsearch index.
    ///
    /// # Arguments
    ///
    /// * `document` - The document to index as a JSON value
    /// * `index_name` - The target index name
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful indexing.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The document cannot be indexed (e.g., mapping conflict)
    async fn post_query(&self, document: &Value, index_name: &str) -> Result<(), anyhow::Error>;

    /// Deletes a document from an Elasticsearch index.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - The document ID to delete
    /// * `index_name` - The target index name
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful deletion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network request fails
    /// - The document does not exist
    async fn delete_query(&self, doc_id: &str, index_name: &str) -> Result<(), anyhow::Error>;

    /// Creates a new Elasticsearch index with settings and mappings.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index to create
    /// * `settings` - Index settings JSON
    /// * `mappings` - Field mappings JSON
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful index creation.
    async fn create_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> Result<(), anyhow::Error>;

    /// Updates settings for an existing index.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index to update
    /// * `settings` - New settings to apply
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful update.
    async fn update_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> Result<(), anyhow::Error>;

    /// Performs bulk indexing of documents.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The target index name
    /// * `documents` - Vector of documents to index
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful bulk indexing.
    async fn bulk_index<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> Result<(), anyhow::Error>;

    /// Atomically swaps an alias from old index to new index.
    ///
    /// # Arguments
    ///
    /// * `alias_name` - The alias name
    /// * `new_index_name` - The new index to point to
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful alias swap.
    async fn swap_alias(&self, alias_name: &str, new_index_name: &str)
    -> Result<(), anyhow::Error>;
}

/// Concrete implementation of the Elasticsearch repository.
///
/// `EsRepositoryImpl` manages the Elasticsearch client connection and
/// provides methods for common operations like search, index, and delete.
///
/// # Connection Pool
///
/// Uses a round-robin multi-node connection pool for high availability:
/// - Distributes requests across all configured nodes
/// - Automatically handles node failures
/// - 30-second timeout per request
///
/// # Authentication
///
/// Supports optional Basic authentication. If `ES_ID` and `ES_PW` environment
/// variables are set and non-empty, Basic auth credentials will be included
/// in all requests.
///
/// # Examples
///
/// ```rust,no_run
/// use crate::repository::EsRepositoryImpl;
///
/// let es_repo = EsRepositoryImpl::new()?;
///
/// // Search for documents
/// let query = serde_json::json!({
///     "query": { "match_all": {} }
/// });
/// let results = es_repo.get_search_query(&query, "my_index").await?;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Getters, Clone)]
pub struct EsRepositoryImpl {
    /// The Elasticsearch client instance.
    ///
    /// This client is thread-safe and can be shared across async tasks.
    es_client: Elasticsearch,
}

impl EsRepositoryImpl {
    /// Creates a new `EsRepositoryImpl` instance.
    ///
    /// Initializes the Elasticsearch client by:
    /// 1. Reading connection configuration from environment variables
    /// 2. Building a multi-node connection pool with round-robin strategy
    /// 3. Configuring authentication if credentials are provided
    /// 4. Setting up a 30-second request timeout
    ///
    /// # Environment Variables
    ///
    /// * `ES_DB_URL` - Required. Comma-separated list of Elasticsearch hosts (e.g., `host1:9200,host2:9200`)
    /// * `ES_ID` - Required but can be empty. Username for Basic authentication
    /// * `ES_PW` - Required but can be empty. Password for Basic authentication
    ///
    /// # Returns
    ///
    /// Returns `Ok(EsRepositoryImpl)` on successful initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any required environment variable is not set
    /// - Host URLs cannot be parsed
    /// - Transport cannot be built
    ///
    /// # Panics
    ///
    /// Panics if `ES_DB_URL`, `ES_ID`, or `ES_PW` environment variables are not set.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use crate::repository::EsRepositoryImpl;
    ///
    /// // Ensure environment variables are set:
    /// // ES_DB_URL=localhost:9200
    /// // ES_ID=elastic
    /// // ES_PW=password
    ///
    /// let es_repo = EsRepositoryImpl::new()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn new() -> anyhow::Result<Self> {
        let app_config: &AppConfig = AppConfig::global();

        // Parse comma-separated host list from environment
        let es_host: Vec<String> = app_config
            .es_db_url()
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        // Load authentication credentials
        let es_id: String = app_config.es_id().to_string();
        let es_pw: String = app_config.es_pw().to_string();

        // Build cluster URLs with http:// prefix
        let cluster_urls: Vec<Url> = es_host
            .iter()
            .map(|host| Url::parse(&format!("http://{}", host)))
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow!("[EsRepositoryImpl::new] {:?}", e))?;

        // Create round-robin connection pool for load balancing
        let conn_pool: MultiNodeConnectionPool =
            MultiNodeConnectionPool::round_robin(cluster_urls, None);

        // Configure transport with 30-second timeout
        let mut builder: TransportBuilder =
            TransportBuilder::new(conn_pool).timeout(Duration::from_secs(30));

        // Add Basic authentication if credentials are provided
        if !es_id.is_empty() && !es_pw.is_empty() {
            builder = builder.auth(EsCredentials::Basic(es_id, es_pw));
        }

        // Build the transport layer
        let transport: Transport = builder
            .build()
            .map_err(|e| anyhow!("[EsRepositoryImpl::new] {:?}", e))?;

        // Create and return the Elasticsearch client
        let es_client: Elasticsearch = Elasticsearch::new(transport);

        Ok(EsRepositoryImpl { es_client })
    }
}

#[async_trait]
impl EsRepository for EsRepositoryImpl {
    /// Executes a search query against an Elasticsearch index.
    ///
    /// Sends the provided query DSL to Elasticsearch and returns the full
    /// response including hits, aggregations, and metadata.
    ///
    /// # Arguments
    ///
    /// * `es_query` - The Elasticsearch query DSL as a JSON value
    /// * `index_name` - The target index name to search
    ///
    /// # Returns
    ///
    /// Returns `Ok(Value)` containing the complete Elasticsearch response.
    ///
    /// # Response Structure
    ///
    /// ```json
    /// {
    ///     "hits": {
    ///         "total": { "value": 100 },
    ///         "hits": [...]
    ///     },
    ///     "aggregations": {...}
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network request fails
    /// - Elasticsearch returns non-2xx status code
    /// - Response body cannot be parsed as JSON
    async fn get_search_query(&self, es_query: &Value, index_name: &str) -> anyhow::Result<Value> {

        // Execute search request against the specified index
        let response: Response = self
            .es_client
            .search(SearchParts::Index(&[index_name]))
            .body(es_query)
            .send()
            .await?;
        
        // Check response status and return appropriate result
        if response.status_code().is_success() {
            let response_body: Value = response.json::<Value>().await?;
            Ok(response_body)
        } else {
            let error_body: String = response.text().await?;
            Err(anyhow!(
                "[EsRepositoryImpl::node_search_query] response status is failed: {:?}",
                error_body
            ))
        }
    }

    /// Indexes a document into an Elasticsearch index.
    ///
    /// Creates a new document in the specified index. Elasticsearch will
    /// automatically generate a document ID if not provided in the document.
    ///
    /// # Arguments
    ///
    /// * `document` - The document to index as a JSON value
    /// * `index_name` - The target index name
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful indexing.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network request fails
    /// - Document violates index mapping (type mismatch)
    /// - Index is read-only or unavailable
    async fn post_query(&self, document: &Value, index_name: &str) -> anyhow::Result<()> {
        // Send index request with the document as body
        let response: Response = self
            .es_client
            .index(IndexParts::Index(index_name))
            .body(document)
            .send()
            .await?;

        // Verify successful indexing
        if response.status_code().is_success() {
            info!("[EsRepositoryImpl::post_query] index_name: {}", index_name);
            Ok(())
        } else {
            let error_message = format!(
                "[EsRepositoryImpl::post_query] Failed to index document: Status Code: {}",
                response.status_code()
            );
            Err(anyhow!(error_message))
        }
    }

    /// Deletes a document from an Elasticsearch index by ID.
    ///
    /// Removes the specified document from the index. The document must exist,
    /// otherwise Elasticsearch will return a 404 error.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - The unique document ID to delete
    /// * `index_name` - The target index name containing the document
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful deletion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network request fails
    /// - Document with specified ID does not exist (404)
    /// - Index is read-only or unavailable
    async fn delete_query(&self, doc_id: &str, index_name: &str) -> anyhow::Result<()> {
        // Send delete request for the specified document
        let response: Response = self
            .es_client
            .delete(DeleteParts::IndexId(index_name, doc_id))
            .send()
            .await?;

        // Verify successful deletion
        if response.status_code().is_success() {
            info!(
                "[EsRepositoryImpl::delete_query] index name: {}, doc_id: {}",
                index_name, doc_id
            );
            Ok(())
        } else {
            let error_message = format!(
                "[EsRepositoryImpl::delete_query] Failed to delete document: Status Code: {}, Document ID: {}",
                response.status_code(),
                doc_id
            );
            Err(anyhow!(error_message))
        }
    }

    async fn create_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()> {
        let body: Value = json!({
            "settings": settings,
            "mappings": mappings
        });

        let response: Response = self
            .es_client
            .indices()
            .create(IndicesCreateParts::Index(index_name))
            .body(body)
            .send()
            .await?;

        if response.status_code().is_success() {
            info!(
                "[EsRepositoryImpl::create_index] Successfully created index: {}",
                index_name
            );
            Ok(())
        } else {
            let error_body = response.text().await?;
            Err(anyhow!(
                "[EsRepositoryImpl::create_index] Failed to create index {}: {}",
                index_name,
                error_body
            ))
        }
    }

    async fn update_index_settings(
        &self,
        index_name: &str,
        settings: &Value,
    ) -> anyhow::Result<()> {
        let response: Response = self
            .es_client
            .indices()
            .put_settings(IndicesPutSettingsParts::Index(&[index_name]))
            .body(settings)
            .send()
            .await?;

        if response.status_code().is_success() {
            info!(
                "[EsRepositoryImpl::update_index_settings] Successfully updated settings for index: {}",
                index_name
            );
            Ok(())
        } else {
            let error_body: String = response.text().await?;
            Err(anyhow!(
                "[EsRepositoryImpl::update_index_settings] Failed to update settings for index {}: {}",
                index_name,
                error_body
            ))
        }
    }

    async fn bulk_index<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
    ) -> anyhow::Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(documents.len() * 2);
        let mut debug_body: Vec<Value> = Vec::with_capacity(documents.len() * 2);

        for doc in documents {
            let index_action: Value = json!({"index": {"_index": index_name}});
            let doc_json: Value = json!(doc);

            debug_body.push(index_action.clone());
            debug_body.push(doc_json.clone());

            // Add the index action
            body.push(index_action.into());
            // Add the document
            body.push(doc_json.into());
        }

        info!("{:?}", debug_body);

        let response: Response = self
            .es_client
            .bulk(BulkParts::None)
            .body(body)
            .send()
            .await?;

        if response.status_code().is_success() {
            let response_body: Value = response.json().await?;

            // Check if there were any errors in the bulk response
            if let Some(errors) = response_body.get("errors") {
                if errors.as_bool() == Some(true) {
                    warn!(
                        "[EsRepositoryImpl::bulk_index] Some documents failed to index: {:?}",
                        response_body.get("items")
                    );
                }
            }

            info!(
                "[EsRepositoryImpl::bulk_index] Successfully bulk indexed documents to: {}",
                index_name
            );
            Ok(())
        } else {
            let error_body: String = response.text().await?;
            Err(anyhow!(
                "[EsRepositoryImpl::bulk_index] Failed to bulk index to {}: {}",
                index_name,
                error_body
            ))
        }
    }

    async fn swap_alias(&self, alias_name: &str, new_index_name: &str) -> anyhow::Result<()> {
        // First, check if the alias exists and get the old index
        let get_alias_response: std::result::Result<Response, elasticsearch::Error> = self
            .es_client
            .indices()
            .get_alias(IndicesGetAliasParts::Name(&[alias_name]))
            .send()
            .await;

        let mut actions: Vec<Value> = Vec::new();

        // If alias exists, remove it from the old index
        if let Ok(response) = get_alias_response {
            if response.status_code().is_success() {
                let body: Value = response.json().await?;

                // Add remove actions for all indices currently using this alias
                if let Some(obj) = body.as_object() {
                    for old_index in obj.keys() {
                        actions.push(json!({
                            "remove": {
                                "index": old_index,
                                "alias": alias_name
                            }
                        }));
                    }
                }
            }
        }

        // Add the alias to the new index
        actions.push(json!({
            "add": {
                "index": new_index_name,
                "alias": alias_name
            }
        }));

        let body: Value = json!({
            "actions": actions
        });

        let response: Response = self
            .es_client
            .indices()
            .update_aliases()
            .body(body)
            .send()
            .await?;

        if response.status_code().is_success() {
            info!(
                "[EsRepositoryImpl::swap_alias] Successfully swapped alias {} to index {}",
                alias_name, new_index_name
            );
            Ok(())
        } else {
            let error_body: String = response.text().await?;
            Err(anyhow!(
                "[EsRepositoryImpl::swap_alias] Failed to swap alias {} to {}: {}",
                alias_name,
                new_index_name,
                error_body
            ))
        }
    }
}
