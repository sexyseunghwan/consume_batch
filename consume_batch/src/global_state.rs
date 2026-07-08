//! Global mutable state for the batch application.
//!
//! This module declares application-wide global variables that require
//! shared read/write access across multiple async tasks.
//!
//! # Design
//!
//! Each variable is wrapped in [`LazyStatic`]`<`[`RwLock`]`<T>>` to allow:
//! - **Lazy initialization** — allocated only on first access
//! - **Multiple concurrent readers** — via `RwLock::read()`
//! - **Exclusive writer** — via `RwLock::write()`
//!
//! # Usage
//!
//! ```rust
//! // Read
//! let is_active = is_spent_detail_indexing_active().await;
//!
//! // Write
//! set_spent_detail_indexing_active(false).await;
//! ```

use crate::common::*;

/// Whether a `spent_detail` full/incremental indexing job is currently running.
///
/// Initialized to `true` at startup.
pub static IS_SPENT_DETAIL_INDEXING_ACTIVE: LazyStatic<RwLock<bool>> = LazyStatic::new(|| RwLock::new(true));

#[allow(dead_code)]
pub static IS_SPENT_DETAIL_CATCHUP_RUNNING: LazyStatic<RwLock<bool>> =
    LazyStatic::new(|| RwLock::new(true));

pub async fn is_spent_detail_indexing_active() -> bool {
    *IS_SPENT_DETAIL_INDEXING_ACTIVE.read().await
}

pub async fn set_spent_detail_indexing_active(value: bool) {
    *IS_SPENT_DETAIL_INDEXING_ACTIVE.write().await = value;
}
