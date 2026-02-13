//! Batch controller implementation.
//!
//! This module provides the controller layer for batch processing operations,
//! acting as the entry point that orchestrates batch service execution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Application                              │
//! │                            │                                    │
//! │                            ▼                                    │
//! │  ┌─────────────────────────────────────────────────────────┐    │
//! │  │                   BatchController                       │    │
//! │  │              (Entry Point / Orchestrator)               │    │
//! │  └─────────────────────────────────────────────────────────┘    │
//! │                            │                                    │
//! │                            ▼                                    │
//! │  ┌─────────────────────────────────────────────────────────┐    │
//! │  │                    BatchService                         │    │
//! │  │           (Business Logic / Scheduling)                 │    │
//! │  └─────────────────────────────────────────────────────────┘    │
//! │                            │                                    │
//! │         ┌──────────────────┼──────────────────┐                 │
//! │         ▼                  ▼                  ▼                 │
//! │  ┌────────────┐    ┌────────────┐    ┌────────────┐             │
//! │  │   MySQL    │    │Elasticsearch│   │  Consume   │             │
//! │  │  Service   │    │  Service    │   │  Service   │             │
//! │  └────────────┘    └────────────┘    └────────────┘             │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Responsibility
//!
//! The controller layer is responsible for:
//! - Providing a clean entry point for batch operations
//! - Handling top-level error logging
//! - Decoupling the application entry point from service implementation
//!
//! # Usage
//!
//! ```rust,no_run
//! use crate::controller::BatchController;
//! use crate::service::BatchServiceImpl;
//!
//! let batch_service = BatchServiceImpl::new(mysql, elastic, consume)?;
//! let controller = BatchController::new(batch_service);
//!
//! // Start the batch processing (runs indefinitely)
//! controller.main_task().await?;
//! ```

use crate::common::*;

use crate::service_trait::batch_service::*;

/// Controller for batch processing operations.
///
/// `BatchController` serves as the entry point for batch operations,
/// wrapping the underlying `BatchService` and providing a clean interface
/// for the application to start batch processing.
///
/// # Type Parameters
///
/// * `B` - Any type implementing the [`BatchService`] trait
///
/// # Design Pattern
///
/// This follows the Controller pattern from MVC/Clean Architecture:
/// - Controllers handle the "what" (entry points, orchestration)
/// - Services handle the "how" (business logic, implementation)
///
/// # Thread Safety
///
/// The controller requires `B: Send + Sync` to ensure safe usage
/// across async tasks and threads.
///
/// # Examples
///
/// ```rust,no_run
/// use crate::controller::BatchController;
/// use crate::service::BatchServiceImpl;
///
/// // Create the batch service with dependencies
/// let batch_service = BatchServiceImpl::new(
///     mysql_service,
///     elastic_service,
///     consume_service,
/// )?;
///
/// // Create controller with the service
/// let controller = BatchController::new(batch_service);
///
/// // Run the batch processing
/// controller.main_task().await?;
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, new)]
pub struct BatchController<B: BatchService> {
    /// The batch service instance that handles actual processing logic.
    ///
    /// This service is responsible for:
    /// - Loading and managing batch schedules
    /// - Running the job scheduler
    /// - Executing batch jobs based on cron expressions
    batch_service: B,
}

impl<B> BatchController<B>
where
    B: BatchService + Send + Sync,
{
    /// Starts the main batch processing task.
    ///
    /// This is the primary entry point for batch operations. It delegates
    /// to the underlying `BatchService::main_batch_task()` and handles
    /// any errors that occur during execution.
    ///
    /// # Execution Flow
    ///
    /// ```text
    /// main_task()
    ///      │
    ///      ▼
    /// batch_service.main_batch_task()
    ///      │
    ///      ├── Success ──► Ok(())
    ///      │
    ///      └── Error ──► Log error ──► Ok(())
    /// ```
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`. Errors from the batch service are logged
    /// but do not propagate up, allowing the application to handle
    /// shutdown gracefully.
    ///
    /// # Error Handling
    ///
    /// Errors are caught and logged at this level rather than propagated.
    /// This design choice allows:
    /// - Graceful error reporting with full context (`{:#}`)
    /// - Application-level control over error handling
    /// - Clean separation between batch execution and error policy
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// let controller = BatchController::new(batch_service);
    ///
    /// // This will run until Ctrl+C is received
    /// controller.main_task().await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub async fn main_task(&self) -> anyhow::Result<()> {
        // Delegate to batch service and handle errors
        match self.batch_service.main_batch_task().await {
            Ok(_) => (),
            Err(e) => {
                error!("[BatchController::main_task] {:#}", e);
            }
        }
        
        Ok(())
    }
}
