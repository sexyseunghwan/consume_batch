//! Configuration module.
//!
//! This module provides centralized configuration management for the application,
//! including environment variables and file-based configurations.
//!
//! # Modules
//!
//! * [`environment`] - Global environment configuration loaded from `.env`
//!
//! # Usage
//!
//! ```rust
//! use crate::config::environment::ENV;
//!
//! // Access configuration values
//! let es_url = ENV.elasticsearch.url();
//! let batch_path = ENV.paths.batch_schedule();
//! ```

pub mod environment;
pub use environment::*;
