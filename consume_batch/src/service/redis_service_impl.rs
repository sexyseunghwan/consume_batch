use crate::common::*;
use crate::repository::redis_repository::*;

use crate::service_trait::redis_service::*;

#[derive(Debug, Getters, Clone, new)]
pub struct RedisServiceImpl<R: RedisRepository> {
    redis_conn: R,
}

#[async_trait]
impl<R: RedisRepository + Send + Sync> RedisService for RedisServiceImpl<R> {
    /// Stores a string value in Redis, optionally with a TTL expiration.
    ///
    /// # Arguments
    ///
    /// * `key` - The Redis key to set
    /// * `value` - The string value to store
    /// * `ttl_seconds` - Optional time-to-live in seconds; if `None`, the key has no expiration
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis operation fails.
    async fn input_string(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<()> {
        match ttl_seconds {
            Some(ttl) => self.redis_conn.input_value_ex(key, value, ttl).await,
            None => self.redis_conn.input_value(key, value).await,
        }
    }

    /// Retrieves a string value from Redis by key.
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
    async fn find_string(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.redis_conn.find_value(key).await
    }
}
