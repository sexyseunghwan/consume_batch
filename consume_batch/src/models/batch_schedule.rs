//! Batch schedule configuration models.
//!
//! This module provides types for deserializing and working with batch schedule
//! configurations defined in TOML format.
//!
//! # File Format
//!
//! The expected TOML structure:
//!
//! ```toml
//! [[batch_schedule]]
//! index_name = "my_index"
//! enabled = true
//! cron_schedule = "0 */5 * * * *"
//! ```

use crate::common::*;
use crate::utils_module::io_utils::read_toml_from_file;

/// Root configuration structure for batch schedules.
///
/// This struct represents the top-level structure of the `batch_schedule.toml` file,
/// containing a list of [`BatchScheduleItem`]s.
///
/// # Examples
///
/// ```rust,no_run
/// use crate::models::BatchScheduleConfig;
///
/// let config = BatchScheduleConfig::load_from_file("config/batch_schedule.toml")?;
/// println!("Loaded {} schedules", config.batch_schedule.len());
/// # Ok::<(), anyhow::Error>(())
/// ```
#[derive(Debug, Deserialize, Clone, Getters)]
#[getset(get = "pub")]
pub struct BatchScheduleConfig {
    /// List of batch schedule items.
    pub batch_schedule: Vec<BatchScheduleItem>,
}

/// A single batch schedule configuration entry.
///
/// Represents an individual batch job schedule with its target index,
/// enabled state, and cron expression.
///
/// # Fields
///
/// * `index_name` - The Elasticsearch index name to process
/// * `enabled` - Whether this schedule is active
/// * `cron_schedule` - Cron expression defining when the batch runs
///
/// # Examples
///
/// ```rust
/// // Access fields via generated getters
/// let item: &BatchScheduleItem = schedule.get(0).unwrap();
/// println!("Index: {}", item.index_name());
/// println!("Enabled: {}", item.enabled());
/// println!("Cron: {}", item.cron_schedule());
/// ```
#[derive(Debug, Deserialize, Clone, Getters)]
#[getset(get = "pub")]
pub struct BatchScheduleItem {
    // The name of the Batch Process
    batch_name: String,

    /// The name of the Elasticsearch index to be processed by this batch job.
    index_name: String,

    /// Whether this batch schedule is currently active.
    ///
    /// When `false`, the scheduler should skip this entry.
    enabled: bool,

    /// Cron expression defining the schedule timing.
    ///
    /// Uses 7-field cron format: `sec min hour day month weekday year`
    ///
    /// # Example
    ///
    /// `"0 */5 * * * * *"` - Run every 5 minutes
    cron_schedule: String,

    relation_topic: String,

    relation_topic_sub: String,
    
    consumer_group: String,

    batch_size: usize,

    /// Whether to apply cron scheduling for this batch job.
    ///
    /// - `true`: Register with cron scheduler, runs periodically based on cron_schedule
    /// - `false`: Run once immediately at startup, then skip scheduling
    cron_schedule_apply: bool,

    mapping_schema: String,

    /// Traffic weight for Blue/Green deployment (0.0 ~ 1.0)
    ///
    /// Determines what percentage of read traffic goes to the new index:
    /// - `0.0`: All traffic to old index (Blue)
    /// - `0.5`: 50% traffic split
    /// - `1.0`: All traffic to new index (Green)
    #[serde(default)]
    traffic_weight: f32,
}

impl BatchScheduleConfig {
    /// Loads batch schedule configuration from a TOML file.
    ///
    /// Reads and parses the specified TOML file into a [`BatchScheduleConfig`].
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    ///
    /// Returns `Ok(BatchScheduleConfig)` on success, or an error if:
    /// - The file cannot be read
    /// - The TOML syntax is invalid
    /// - The structure doesn't match the expected schema
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The file does not exist or cannot be read
    /// - The file contains invalid TOML syntax
    /// - Required fields are missing or have incorrect types
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use crate::models::BatchScheduleConfig;
    ///
    /// let config = BatchScheduleConfig::load_from_file("src/config/batch_schedule.toml")?;
    /// for schedule in &config.batch_schedule {
    ///     println!("{}: {}", schedule.index_name(), schedule.enabled());
    /// }
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn load_from_file(path: &str) -> Result<Self> {
        read_toml_from_file(path)
    }

    /// Returns only the enabled batch schedules.
    ///
    /// Filters the schedule list to return references to items where
    /// `enabled` is `true`.
    ///
    /// # Returns
    ///
    /// A vector of references to enabled [`BatchScheduleItem`]s.
    /// Returns an empty vector if no schedules are enabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let config = BatchScheduleConfig::load_from_file("batch_schedule.toml")?;
    /// let enabled = config.get_enabled_schedules();
    ///
    /// println!("Active schedules: {}", enabled.len());
    /// for schedule in enabled {
    ///     println!("  - {}", schedule.index_name());
    /// }
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn get_enabled_schedules(&self) -> Vec<&BatchScheduleItem> {
        self.batch_schedule
            .iter()
            .filter(|item| item.enabled)
            .collect()
    }
}
