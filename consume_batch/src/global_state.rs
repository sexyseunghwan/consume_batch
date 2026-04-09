//! Global mutable state for the batch application.
//!
//! This module declares application-wide global variables that require
//! shared read/write access across multiple async tasks.
//!
//! # Design
//!
//! Each variable is wrapped in [`once_lazy`]`<`[`RwLock`]`<T>>` to allow:
//! - **Lazy initialization** — allocated only on first access
//! - **Multiple concurrent readers** — via `RwLock::read()`
//! - **Exclusive writer** — via `RwLock::write()`
//!
//! # Usage
//!
//! ```rust
//! // Read
//! let val = get_spent_detail_indexing().await;
//!
//! // Write
//! set_spent_detail_indexing(42).await;
//! ```

use crate::common::*;

/// Tracks the number of `spent_detail` documents indexed in the current batch run.
///
/// Initialized to `0` at startup and updated after each indexing cycle.
pub static SPENT_DETAIL_INDEXING: once_lazy<RwLock<bool>> = once_lazy::new(|| RwLock::new(true));

pub static SPENT_DETAIL_INDEXING_CATCHUP: once_lazy<RwLock<bool>> = once_lazy::new(|| RwLock::new(true));

/// Returns the current value of [`SPENT_DETAIL_INDEXING`].
pub async fn get_spent_detail_indexing() -> bool {
    *SPENT_DETAIL_INDEXING.read().await
}

/// Sets [`SPENT_DETAIL_INDEXING`] to the given `value`.
pub async fn set_spent_detail_indexing(value: bool) {
    *SPENT_DETAIL_INDEXING.write().await = value;
}

// pub async fn get_spent_detail_indexing_catchup() -> bool {
//     *SPENT_DETAIL_INDEXING_CATCHUP.read().await
// }

// pub async fn set_spent_detail_indexing_catchup(value: bool) {
//     *SPENT_DETAIL_INDEXING_CATCHUP.write().await = value;
// }