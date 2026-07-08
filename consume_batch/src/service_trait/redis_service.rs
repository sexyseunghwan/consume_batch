use crate::common::*;

#[async_trait]
pub trait RedisService {
    async fn input_value(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: Option<u64>,
    ) -> anyhow::Result<()>;

    async fn find_value(&self, key: &str) -> anyhow::Result<Option<String>>;
}
