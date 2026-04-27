use crate::common::*;

#[async_trait]
pub trait CliService {
    /// Starts the Unix socket server used by the interactive CLI client.
    async fn initialize_socket_server(&self) -> anyhow::Result<()>;
}
