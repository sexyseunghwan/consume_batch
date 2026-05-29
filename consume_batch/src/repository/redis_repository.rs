use crate::common::*;

use crate::app_config::*;

use redis::{
    AsyncCommands, Client as redisClient, RedisError, aio::MultiplexedConnection,
    cluster::ClusterClient, cluster_async::ClusterConnection,
};

/// Enum to represent different types of Redis connections
pub enum RedisConnectionType {
    Single(MultiplexedConnection),
    Cluster(ClusterConnection),
}

/// Redis repository trait defining common Redis operations
#[async_trait]
pub trait RedisRepository {
    /// Get a value by key
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Result<Option<String>, anyhow::Error>` - The value if exists, None otherwise
    async fn find_value(&self, key: &str) -> anyhow::Result<Option<String>>;

    /// Set a key-value pair
    ///
    /// # Arguments
    /// * `key` - The key to set
    /// * `value` - The value to set
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if set succeeds
    async fn input_value(&self, key: &str, value: &str) -> anyhow::Result<()>;

    /// Set a key-value pair with expiration time
    ///
    /// # Arguments
    /// * `key` - The key to set
    /// * `value` - The value to set
    /// * `seconds` - Expiration time in seconds
    ///
    /// # Returns
    /// * `Result<(), anyhow::Error>` - Ok if set succeeds
    async fn input_value_ex(&self, key: &str, value: &str, seconds: u64) -> anyhow::Result<()>;
}

/// Redis repository implementation
pub struct RedisRepositoryImpl {
    conn: RedisConnectionType,
}

impl RedisRepositoryImpl {
    /// Create a new Redis repository instance
    /// Supports both single node and cluster modes based on REDIS_URL format
    ///
    /// # Environment Variables
    /// * `REDIS_URL` - Redis connection URL
    ///   - Single node: "redis://127.0.0.1:6379"
    ///   - Cluster: "redis://node1:6379,redis://node2:6379,redis://node3:6379"
    ///
    /// # Returns
    /// * `Result<Self, anyhow::Error>` - New instance or error
    pub async fn new() -> anyhow::Result<Self> {
        let cfg: &AppConfig = AppConfig::get_global()?;

        let redis_url: &str = cfg.redis_url();

        // Check if the URL contains multiple nodes (cluster mode)
        let conn: RedisConnectionType = if redis_url.contains(',') {
            // Cluster mode
            let nodes: Vec<&str> = redis_url.split(',').collect();
            let node_count: usize = nodes.len();
            let cluster_client: ClusterClient = ClusterClient::new(nodes).map_err(|e| {
                anyhow!(
                    "[RedisRepositoryImpl::new] Failed to create Redis cluster client: {:?}",
                    e
                )
            })?;

            let cluster_conn: ClusterConnection =
                cluster_client.get_async_connection().await.map_err(|e| {
                    anyhow!(
                        "[RedisRepositoryImpl::new] Failed to connect to Redis cluster: {:?}",
                        e
                    )
                })?;

            info!(
                "[RedisRepositoryImpl::new] Connected to Redis cluster with {} nodes",
                node_count
            );
            RedisConnectionType::Cluster(cluster_conn)
        } else {
            // Single node mode
            let client: redisClient = redisClient::open(redis_url).map_err(|e| {
                anyhow!(
                    "[RedisRepositoryImpl::new] Failed to create Redis client: {:?}",
                    e
                )
            })?;

            let single_conn: MultiplexedConnection = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| {
                    anyhow!(
                        "[RedisRepositoryImpl::new] Failed to connect to Redis: {:?}",
                        e
                    )
                })?;

            info!("[RedisRepositoryImpl::new] Connected to Redis single node");
            RedisConnectionType::Single(single_conn)
        };

        Ok(Self { conn })
    }
}

#[async_trait]
impl RedisRepository for RedisRepositoryImpl {
    /// Retrieves a value from Redis for the given key, supporting both single-node and cluster modes.
    ///
    /// # Arguments
    ///
    /// * `key` - The Redis key to look up
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(String))` if the key exists, or `Ok(None)` if it does not.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis operation fails.
    async fn find_value(&self, key: &str) -> anyhow::Result<Option<String>> {
        match &self.conn {
            RedisConnectionType::Single(conn) => {
                let mut conn: MultiplexedConnection = conn.clone();
                let result: Option<String> = conn.get(key).await.map_err(|e: RedisError| {
                    anyhow!(
                        "[RedisRepositoryImpl::find_value] Failed to get key '{}': {:?}",
                        key,
                        e
                    )
                })?;
                Ok(result)
            }
            RedisConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                let result: Option<String> = conn.get(key).await.map_err(|e: RedisError| {
                    anyhow!(
                        "[RedisRepositoryImpl::find_value] Failed to get key '{}': {:?}",
                        key,
                        e
                    )
                })?;
                Ok(result)
            }
        }
    }

    /// Stores a key-value pair in Redis without an expiration, supporting both single-node and cluster modes.
    ///
    /// # Arguments
    ///
    /// * `key` - The Redis key to set
    /// * `value` - The string value to store
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis operation fails.
    async fn input_value(&self, key: &str, value: &str) -> anyhow::Result<()> {
        match &self.conn {
            RedisConnectionType::Single(conn) => {
                let mut conn = conn.clone();
                conn.set::<_, _, ()>(key, value)
                    .await
                    .map_err(|e: RedisError| {
                        anyhow!(
                            "[RedisRepositoryImpl::input_value] Failed to set key '{}': {:?}",
                            key,
                            e
                        )
                    })?;
                Ok(())
            }
            RedisConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.set::<_, _, ()>(key, value)
                    .await
                    .map_err(|e: RedisError| {
                        anyhow!(
                            "[RedisRepositoryImpl::input_value] Failed to set key '{}': {:?}",
                            key,
                            e
                        )
                    })?;
                Ok(())
            }
        }
    }

    /// Stores a key-value pair in Redis with a TTL expiration, supporting both single-node and cluster modes.
    ///
    /// # Arguments
    ///
    /// * `key` - The Redis key to set
    /// * `value` - The string value to store
    /// * `seconds` - Time-to-live in seconds after which the key expires
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis operation fails.
    async fn input_value_ex(&self, key: &str, value: &str, seconds: u64) -> anyhow::Result<()> {
        match &self.conn {
            RedisConnectionType::Single(conn) => {
                let mut conn = conn.clone();
                conn.set_ex::<_, _, ()>(key, value, seconds)
                    .await
                    .map_err(|e: RedisError| anyhow!("[RedisRepositoryImpl::input_value_ex] Failed to set key '{}' with expiration: {:?}", key, e))?;
                Ok(())
            }
            RedisConnectionType::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.set_ex::<_, _, ()>(key, value, seconds)
                    .await
                    .map_err(|e: RedisError| anyhow!("[RedisRepositoryImpl::input_value_ex] Failed to set key '{}' with expiration: {:?}", key, e))?;
                Ok(())
            }
        }
    }
}
