//! Global environment configuration module.
//!
//! This module provides a centralized, lazily-initialized configuration
//! loaded from environment variables (`.env` file).
//!
//! # Usage
//!
//! Access configuration values through the global [`ENV`] static:
//!
//! ```rust
//! use crate::config::environment::ENV;
//!
//! let es_url = ENV.elasticsearch.url();
//! let batch_path = ENV.paths.batch_schedule();
//! ```
//!
//! # Initialization
//!
//! Configuration is loaded lazily on first access. Ensure `dotenv().ok()`
//! is called in `main()` before accessing [`ENV`].

use crate::common::*;

// ============================================================================
// Path Configuration
// ============================================================================

/// File path configurations loaded from environment variables.
///
/// Contains paths to configuration files and other file-based resources.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct PathConfig {
    /// Path to the batch schedule TOML configuration file.
    ///
    /// Environment variable: `BATCH_SCHEDULE`
    batch_schedule: String,
}

impl PathConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            batch_schedule: env::var("BATCH_SCHEDULE")
                .unwrap_or_else(|_| "./config/batch_schedule.toml".to_string()),
        })
    }
}

// ============================================================================
// Elasticsearch Configuration
// ============================================================================

/// Elasticsearch connection configuration.
///
/// Contains all necessary parameters for connecting to an Elasticsearch cluster.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct ElasticsearchConfig {
    /// Comma-separated list of Elasticsearch node URLs.
    ///
    /// Environment variable: `ES_DB_URL`
    ///
    /// # Example
    ///
    /// `"192.168.0.89:9200,192.168.0.90:9200,192.168.0.91:9200"`
    url: String,

    /// Elasticsearch authentication username.
    ///
    /// Environment variable: `ES_ID`
    id: String,

    /// Elasticsearch authentication password.
    ///
    /// Environment variable: `ES_PW`
    password: String,
}

impl ElasticsearchConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            url: env::var("ES_DB_URL").context("ES_DB_URL must be set")?,
            id: env::var("ES_ID").context("ES_ID must be set")?,
            password: env::var("ES_PW").context("ES_PW must be set")?,
        })
    }

    /// Returns a list of Elasticsearch node URLs.
    ///
    /// Parses the comma-separated URL string into individual URLs.
    ///
    /// # Returns
    ///
    /// A vector of URL strings for each node in the cluster.
    pub fn urls(&self) -> Vec<String> {
        self.url.split(',').map(|s| s.to_string()).collect()
    }
}

// ============================================================================
// Kafka Configuration
// ============================================================================

/// Kafka broker configuration.
///
/// Contains connection parameters for Apache Kafka.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct KafkaConfig {
    /// Comma-separated list of Kafka broker addresses.
    ///
    /// Environment variable: `KAFKA_HOST`
    ///
    /// # Example
    ///
    /// `"192.168.0.89:9092,192.168.0.90:9092,192.168.0.91:9092"`
    host: String,

    /// Consumer group ID.
    ///
    /// Environment variable: `KAFKA_GROUP_ID`
    group_id: String,

    /// Security protocol for Kafka connection.
    ///
    /// Environment variable: `KAFKA_SECURITY_PROTOCOL`
    ///
    /// # Example
    ///
    /// `"SASL_PLAINTEXT"` or `"SASL_SSL"`
    security_protocol: Option<String>,

    /// SASL mechanism for authentication.
    ///
    /// Environment variable: `KAFKA_SASL_MECHANISM`
    ///
    /// # Example
    ///
    /// `"PLAIN"` or `"SCRAM-SHA-256"`
    sasl_mechanism: Option<String>,

    /// SASL username for authentication.
    ///
    /// Environment variable: `KAFKA_SASL_USERNAME`
    sasl_username: Option<String>,

    /// SASL password for authentication.
    ///
    /// Environment variable: `KAFKA_SASL_PASSWORD`
    sasl_password: Option<String>,
}

impl KafkaConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: env::var("KAFKA_HOST")
                .context("[KafkaConfig::from_env] KAFKA_HOST must be set")?,
            group_id: env::var("KAFKA_GROUP_ID")
                .context("[KafkaConfig::from_env] KAFKA_GROUP_ID must be set")?,
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL").ok(),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_username: env::var("KAFKA_SASL_USERNAME").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
        })
    }

    /// Returns true if SASL authentication is configured.
    pub fn is_sasl_enabled(&self) -> bool {
        self.security_protocol.is_some()
            && self.sasl_mechanism.is_some()
            && self.sasl_username.is_some()
            && self.sasl_password.is_some()
    }
}

// ============================================================================
// MySQL Configuration
// ============================================================================

/// MySQL database configuration.
///
/// Contains connection parameters for MySQL database.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct MySqlConfig {
    /// MySQL server host and port.
    ///
    /// Environment variable: `MY_SQL_HOST`
    host: String,

    /// Full database connection URL.
    ///
    /// Environment variable: `DATABASE_URL`
    ///
    /// # Example
    ///
    /// `"mysql://user:password@host:port/database"`
    database_url: String,
}

impl MySqlConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: env::var("MY_SQL_HOST").context("MY_SQL_HOST must be set")?,
            database_url: env::var("DATABASE_URL").context("DATABASE_URL must be set")?,
        })
    }
}

// ============================================================================
// Telegram Configuration
// ============================================================================

/// Telegram bot configuration.
///
/// Contains authentication token for Telegram bot API.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct TelegramConfig {
    /// Telegram bot API token.
    ///
    /// Environment variable: `TELOXIDE_TOKEN`
    token: String,
}

impl TelegramConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            token: env::var("TELOXIDE_TOKEN").context("TELOXIDE_TOKEN must be set")?,
        })
    }
}

// ============================================================================
// Batch Configuration
// ============================================================================

/// Batch processing configuration.
///
/// Contains settings for batch job execution.
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct BatchConfig {
    /// Number of records to process per batch.
    ///
    /// Environment variable: `BATCH_SIZE`
    batch_size: usize,
}

impl BatchConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .context("BATCH_SIZE must be a valid number")?,
        })
    }
}

// ============================================================================
// Root Environment Configuration
// ============================================================================

/// Root configuration structure containing all environment settings.
///
/// This struct aggregates all configuration sections and is accessed
/// through the global [`ENV`] static variable.
///
/// # Sections
///
/// * [`paths`](PathConfig) - File path configurations
/// * [`elasticsearch`](ElasticsearchConfig) - Elasticsearch connection settings
/// * [`kafka`](KafkaConfig) - Kafka broker settings
/// * [`mysql`](MySqlConfig) - MySQL database settings
/// * [`telegram`](TelegramConfig) - Telegram bot settings
/// * [`batch`](BatchConfig) - Batch processing settings
///
/// # Examples
///
/// ```rust
/// use crate::config::environment::ENV;
///
/// // Access Elasticsearch URL
/// let es_urls = ENV.elasticsearch.urls();
///
/// // Access batch schedule path
/// let schedule_path = ENV.paths.batch_schedule();
///
/// // Access batch size
/// let batch_size = *ENV.batch.batch_size();
/// ```
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct Environment {
    /// File path configurations.
    paths: PathConfig,

    /// Elasticsearch connection configuration.
    elasticsearch: ElasticsearchConfig,

    /// Kafka broker configuration.
    kafka: KafkaConfig,

    /// MySQL database configuration.
    mysql: MySqlConfig,

    /// Telegram bot configuration.
    telegram: TelegramConfig,

    /// Batch processing configuration.
    batch: BatchConfig,
}

impl Environment {
    /// Loads all configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if any required environment variable is missing
    /// or has an invalid value.
    fn load() -> Result<Self> {
        Ok(Self {
            paths: PathConfig::from_env()?,
            elasticsearch: ElasticsearchConfig::from_env()?,
            kafka: KafkaConfig::from_env()?,
            mysql: MySqlConfig::from_env()?,
            telegram: TelegramConfig::from_env()?,
            batch: BatchConfig::from_env()?,
        })
    }
}

// ============================================================================
// Global Static Instance
// ============================================================================

/// Global environment configuration instance.
///
/// Lazily initialized on first access. Panics if environment variables
/// are missing or invalid.
///
/// # Panics
///
/// Panics during initialization if required environment variables are
/// not set or have invalid values. Check logs for specific missing variables.
///
/// # Examples
///
/// ```rust
/// use crate::config::environment::ENV;
///
/// // Access any configuration section
/// println!("ES URL: {}", ENV.elasticsearch.url());
/// println!("Kafka hosts: {:?}", ENV.kafka.brokers());
/// println!("Batch size: {}", ENV.batch.batch_size());
/// ```
pub static ENV: once_lazy<Environment> = once_lazy::new(|| {
    Environment::load()
        .unwrap_or_else(|e| panic!("Failed to load environment configuration: {}", e))
});

/// Constant for batch schedule path (for backwards compatibility).
///
/// Prefer using `ENV.paths.batch_schedule()` instead.
pub static BATCH_SCHEDULE_PATH: once_lazy<String> =
    once_lazy::new(|| ENV.paths.batch_schedule().clone());
