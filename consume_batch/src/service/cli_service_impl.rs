//! CLI service implementation.
//!
//! This module provides the Unix socket server that allows interactive,
//! on-demand batch job execution while the main service is running.
//!
//! # Architecture
//!
//! ```text
//! Service Mode
//!      │
//!      ├── BatchService (scheduler + cron jobs)
//!      │
//!      └── CliServiceImpl (Unix socket server)
//!               │
//!               ├── accept connection
//!               ├── send menu
//!               ├── read user selection
//!               └── batch_service.run_batch(schedule_item)
//! ```
//!
//! # Usage
//!
//! The socket server listens on the path configured in `AppConfig::socket_path`.
//! CLI clients connect via `--cli` flag and interact through the socket.

use crate::{app_config::AppConfig, common::*, models::batch_schedule::*};

use crate::service_trait::{batch_service::BatchService, cli_service::CliService};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

/// CLI service implementation that manages the Unix domain socket server.
///
/// `CliServiceImpl<B>` listens for incoming socket connections and delegates
/// batch execution to the underlying `BatchService`.
///
/// # Type Parameters
///
/// * `B` - Any type implementing the [`BatchService`] trait
#[derive(Debug, Clone)]
pub struct CliServiceImpl<B>
where
    B: BatchService,
{
    /// The batch service used to execute selected jobs.
    batch_service: Arc<B>,

    /// Schedule configuration used to build the interactive menu.
    schedule_config: BatchScheduleConfig,
}

impl<B> CliServiceImpl<B>
where
    B: BatchService + Send + Sync + 'static,
{
    /// Creates a new `CliServiceImpl` instance.
    ///
    /// # Arguments
    ///
    /// * `batch_service` - Shared reference to the batch service
    /// * `schedule_config` - Loaded batch schedule configuration for menu generation
    pub fn new(batch_service: Arc<B>, schedule_config: BatchScheduleConfig) -> Self {
        Self {
            batch_service,
            schedule_config,
        }
    }

    /// Handles a single socket connection from a CLI client.
    ///
    /// Displays a numbered menu of available batch jobs and executes the
    /// selected job via `batch_service.run_batch()`. Exits when the client
    /// sends `0` or `q`.
    async fn handle_socket_connection(
        stream: tokio::net::UnixStream,
        batch_service: Arc<B>,
        schedule_config: BatchScheduleConfig,
    ) -> anyhow::Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut reader: tokio::io::BufReader<tokio::net::unix::OwnedReadHalf> =
            tokio::io::BufReader::new(reader);

        let batch_items: Vec<&BatchScheduleItem> = schedule_config.get_enabled_schedules();

        loop {
            // Send numbered batch menu to client
            let mut menu: String =
                String::from("\n==============================\nSelect a batch to execute:\n");

            for (i, item) in batch_items.iter().enumerate() {
                menu.push_str(&format!("  {}. {}\n", i + 1, item.batch_name()));
            }
            menu.push_str("  0. Exit\n");
            menu.push_str("==============================\n");
            menu.push_str("Input:");

            writer.write_all(menu.as_bytes()).await?;

            // Receive user input (including line break)
            let mut line: String = String::new();
            let n: usize = reader.read_line(&mut line).await?;

            if n == 0 {
                break; // Client disconnected
            }

            let input: &str = line.trim();

            if input == "0" || input.eq_ignore_ascii_case("q") {
                writer.write_all("\nExiting.\n".as_bytes()).await?;
                break;
            }

            match input.parse::<usize>() {
                Ok(num) if num >= 1 && num <= batch_items.len() => {
                    let schedule_item: &BatchScheduleItem = &batch_items[num - 1];
                    let batch_name: &str = schedule_item.batch_name();

                    info!(
                        "[CliServiceImpl::handle_socket_connection] CLI triggered: {}",
                        batch_name
                    );

                    writer
                        .write_all(
                            format!("\n[{}] Batch execution in progress...\n", batch_name)
                                .as_bytes(),
                        )
                        .await?;

                    match batch_service.run_batch(schedule_item).await {
                        Ok(()) => {
                            writer
                                .write_all(format!("[{}] Complete.\n", batch_name).as_bytes())
                                .await?;
                        }
                        Err(e) => {
                            writer
                                .write_all(format!("[{}] Failed: {}\n", batch_name, e).as_bytes())
                                .await?;
                        }
                    }
                }
                _ => {
                    writer
                        .write_all(
                            format!(
                                "Please enter the correct number. (1-{})\n",
                                batch_items.len()
                            )
                            .as_bytes(),
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<B> CliService for CliServiceImpl<B>
where
    B: BatchService + Send + Sync + 'static,
{
    /// Starts the Unix domain socket server for CLI batch execution.
    ///
    /// Listens on the path configured in `AppConfig::socket_path` and
    /// spawns a separate task for each incoming connection.
    async fn start_socket_server(&self) -> anyhow::Result<()> {
        let socket_path: &str = AppConfig::global().socket_path();

        // Remove existing socket file to handle unclean shutdowns
        let _ = std::fs::remove_file(socket_path);

        let listener: UnixListener = UnixListener::bind(socket_path)
            .context("[CliServiceImpl::start_socket_server] Failed to bind socket")?;
        
        info!(
            "[CliServiceImpl::start_socket_server] CLI socket server listening on {}",
            socket_path
        );
        
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("[CliServiceImpl::start_socket_server] Failed to accept connection")?;

            let batch: Arc<B> = Arc::clone(&self.batch_service);
            let config: BatchScheduleConfig = self.schedule_config.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_socket_connection(stream, batch, config).await {
                    error!(
                        "[CliServiceImpl::handle_socket_connection] Connection error: {:#}",
                        e
                    );
                }
            });
        }
    }
}
