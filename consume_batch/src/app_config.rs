use crate::common::*;

/// Global configuration struct for environment variables
/// This struct is thread-safe and can be accessed from multiple threads concurrently
#[derive(Debug, Clone, Getters)]
#[getset(get = "pub")]
pub struct AppConfig {
    /// Telegram bot token information.
    pub teloxide_token: String,
    /// Elasticsearch URL
    pub es_db_url: String,
    /// Elasticsearch username
    pub es_id: String,
    /// Elasticsearch password
    pub es_pw: String,

    pub kafka_host: String,
    pub kafka_group_id: String,
    pub kafka_security_protocol: Option<String>,
    pub kafka_sasl_mechanism: Option<String>,
    pub kafka_sasl_username: Option<String>,
    pub kafka_sasl_password: Option<String>,
    pub my_sql_host: String,
    pub database_url: String,
    pub batch_size: usize,
    pub batch_schedule: String,

    pub es_spent_detail: String,
    pub es_spent_type: String,
}

/// Global static instance of AppConfig
/// This is initialized once and can be safely accessed from multiple threads
static APP_CONFIG: normalOnceCell<AppConfig> = normalOnceCell::new();

impl AppConfig {
    /// Initialize the global configuration from environment variables
    /// This should be called once at application startup
    ///
    /// # Returns
    /// * `Result<(), String>` - Ok if initialization succeeds, Err with message if fails
    ///
    /// # Example
    /// ```
    /// use consume_alert_rust::config::AppConfig;
    ///
    /// fn main() {
    ///     AppConfig::init().expect("Failed to initialize config");
    ///     let config = AppConfig::global();
    ///     println!("Consume topic: {}", config.consume_topic);
    /// }
    /// ```
    pub fn init() -> Result<(), String> {
        dotenv::dotenv().ok();

        let config: AppConfig = AppConfig {
            teloxide_token: env::var("TELOXIDE_TOKEN")
                .map_err(|_| "TELOXIDE_TOKEN not found in environment".to_string())?,
            es_db_url: env::var("ES_DB_URL")
                .map_err(|_| "ES_DB_URL not found in environment".to_string())?,
            es_id: env::var("ES_ID").map_err(|_| "ES_ID not found in environment".to_string())?,
            es_pw: env::var("ES_PW").map_err(|_| "ES_PW not found in environment".to_string())?,
            kafka_host: env::var("KAFKA_HOST")
                .map_err(|_| "KAFKA_HOST not found in environment".to_string())?,
            kafka_group_id: env::var("KAFKA_GROUP_ID")
                .map_err(|_| "KAFKA_GROUP_ID not found in environment".to_string())?,
            kafka_security_protocol: env::var("KAFKA_SECURITY_PROTOCOL").ok(),
            kafka_sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            kafka_sasl_username: env::var("KAFKA_SASL_USERNAME").ok(),
            kafka_sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
            my_sql_host: env::var("MY_SQL_HOST")
                .map_err(|_| "MY_SQL_HOST not found in environment".to_string())?,
            database_url: env::var("DATABASE_URL")
                .map_err(|_| "DATABASE_URL not found in environment".to_string())?,
            batch_size: env::var("BATCH_SIZE")
                .map_err(|_| "BATCH_SIZE not found in environment".to_string())?
                .parse::<usize>()
                .map_err(|_| "BATCH_SIZE must be a valid number".to_string())?,
            batch_schedule: env::var("BATCH_SCHEDULE")
                .map_err(|_| "BATCH_SCHEDULE not found in environment".to_string())?,
            es_spent_detail: env::var("ES_SPENT_DETAIL")
                .map_err(|_| "ES_SPENT_DETAIL not found in environment".to_string())?,
            es_spent_type: env::var("ES_SPENT_TYPE")
                .map_err(|_| "ES_SPENT_TYPE not found in environment".to_string())?,
        };

        APP_CONFIG
            .set(config)
            .map_err(|_| "AppConfig already initialized".to_string())
    }

    /// Get a reference to the global configuration
    ///
    /// # Panics
    /// Panics if the configuration has not been initialized with `AppConfig::init()`
    ///
    /// # Returns
    /// * `&'static AppConfig` - Reference to the global configuration
    ///
    /// # Example
    /// ```
    /// use consume_alert_rust::config::AppConfig;
    ///
    /// let config = AppConfig::global();
    /// println!("Topic: {}", config.consume_topic);
    /// ```
    pub fn global() -> &'static AppConfig {
        APP_CONFIG
            .get()
            .expect("AppConfig not initialized. Call AppConfig::init() first.")
    }

    /// Try to get a reference to the global configuration
    /// Returns None if not initialized yet
    ///
    /// # Returns
    /// * `Option<&'static AppConfig>` - Some if initialized, None otherwise
    pub fn try_global() -> Option<&'static AppConfig> {
        APP_CONFIG.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_access() {
        // Note: This test requires .env file to be present
        if AppConfig::try_global().is_none() {
            let _ = AppConfig::init();
        }

        let config: &AppConfig = AppConfig::global();
        // assert!(!config.produce_topic.is_empty());
        // assert!(!config.kafka_brokers.is_empty());
    }
}
