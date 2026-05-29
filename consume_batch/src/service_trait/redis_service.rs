use crate::common::*;

#[async_trait]
pub trait RedisService {
    async fn input_string(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<()>;

    async fn find_string(&self, key: &str) -> anyhow::Result<Option<String>>;
}
