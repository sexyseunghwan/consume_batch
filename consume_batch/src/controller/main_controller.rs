//! Main application controller.
//!
//! This module coordinates the two top-level services:
//! - [`BatchService`]: cron scheduler + batch job execution
//! - [`CliService`]: Unix socket server for on-demand CLI execution
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                   MainController                    │
//! │                                                     │
//! │  ┌──────────────────┐  ┌──────────────────────┐     │
//! │  │   BatchService   │  │     CliService       │     │
//! │  │  (Scheduler /    │  │  (Unix Socket Server)│     │
//! │  │   Cron Jobs)     │  │  (On-demand trigger) │     │
//! │  └──────────────────┘  └──────────────────────┘     │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! let controller = MainController::new(batch_service, Arc::new(cli_service));
//! controller.main_task().await?;
//! ```

use crate::common::*;

use crate::service_trait::{batch_service::BatchService, cli_service::CliService};

/// Top-level controller that owns and coordinates both the batch scheduler
/// and the CLI socket server.
///
/// # Type Parameters
///
/// * `B` - Any type implementing [`BatchService`]
/// * `CS` - Any type implementing [`CliService`]
///
/// # Thread Safety
///
/// `cli_service` is stored as `Arc<CS>` so it can be moved into a spawned task
/// while the controller retains shared ownership.
#[derive(Debug, new)]
pub struct MainController<B, CS>
where
    B: BatchService,
    CS: CliService,
{
    /// Handles cron scheduling and batch job execution.
    batch_service: B,

    /// Handles the Unix socket server for interactive CLI batch triggering.
    cli_service: Arc<CS>,
}

impl<B, CS> MainController<B, CS>
where
    B: BatchService + Send + Sync,
    CS: CliService + Send + Sync + 'static,
{
    /// Starts both the CLI socket server and the batch scheduler.
    ///
    /// The CLI service is spawned as a background task, while the batch service
    /// runs in the foreground and blocks until a Ctrl+C shutdown signal is received.
    ///
    /// # Execution Flow
    ///
    /// ```text
    /// main_task()
    ///      │
    ///      ├── tokio::spawn ──► cli_service.start_socket_server()  [background]
    ///      │
    ///      └── batch_service.main_batch_task()  [foreground, blocks until Ctrl+C]
    /// ```
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`. Errors from either service are logged but not propagated.
    pub async fn main_task(&self) -> anyhow::Result<()> {
        // Spawn CLI socket server in the background
        {
            let cli_service: Arc<CS> = Arc::clone(&self.cli_service);

            tokio::spawn(async move {
                if let Err(e) = cli_service.start_socket_server().await {
                    error!(
                        "[MainController::main_task] CLI socket server error: {:#}",
                        e
                    );
                }
            });
        }

        // Run batch scheduler in the foreground (blocks until Ctrl+C)
        match self.batch_service.main_batch_task().await {
            Ok(_) => (),
            Err(e) => {
                error!("[MainController::main_task] {:#}", e);
            }
        }

        Ok(())
    }
}