use crate::common::*;

#[async_trait]
pub trait CliService {
    async fn start_socket_server(&self) -> anyhow::Result<()>;
}
