//! MySQL repository implementation.
//!
//! This module provides the data access layer for MySQL database operations
//! using SeaORM as the underlying ORM framework.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     MysqlRepositoryImpl                         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │              DatabaseConnection (SeaORM)                 │   │
//! │  │            (Connection Pool Managed)                     │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                            │                                    │
//! │         ┌──────────────────┼──────────────────┐                │
//! │         ▼                  ▼                  ▼                │
//! │  ┌────────────┐    ┌────────────┐    ┌─────────────────┐      │
//! │  │   Insert   │    │   Bulk     │    │  Transactional  │      │
//! │  │   Single   │    │   Insert   │    │  Bulk Insert    │      │
//! │  └────────────┘    └────────────┘    └─────────────────┘      │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Generic Operations
//!
//! All insert operations are generic over SeaORM's `ActiveModel` trait:
//! - Works with any entity that implements `ActiveModelTrait`
//! - No code duplication for different table types
//! - Type-safe at compile time
//!
//! # Transaction Support
//!
//! The repository provides transactional bulk insert for atomic operations:
//! - All-or-nothing semantics
//! - Automatic rollback on failure
//! - Performance optimized with single query execution
//!
//! # Environment Variables
//!
//! | Variable       | Description                    | Example                                      |
//! |----------------|--------------------------------|----------------------------------------------|
//! | `DATABASE_URL` | MySQL connection string        | `mysql://user:pass@localhost:3306/db_name`   |

use crate::common::*;

use sea_orm::{ActiveModelBehavior, IntoActiveModel};

use crate::app_config::AppConfig;

//use crate::app_config::{ENV, MySqlConfig};

/// Trait defining MySQL repository operations.
///
/// This trait abstracts the MySQL data access layer using SeaORM,
/// providing generic methods that work with any entity type.
///
/// # Type Parameters
///
/// All methods are generic over `A` where:
/// - `A: ActiveModelTrait` - SeaORM's active model for insert/update
/// - `A: ActiveModelBehavior` - Lifecycle hooks support
/// - `A: Send + 'static` - Safe to send across threads
///
/// # Implementors
///
/// - [`MysqlRepositoryImpl`] - Production implementation with real DB connection
#[async_trait]
pub trait MysqlRepository {
    /// Inserts a single record into the database.
    ///
    /// Generic insert function that works with any SeaORM ActiveModel,
    /// allowing insertion of any entity type without code duplication.
    ///
    /// # Type Parameters
    ///
    /// * `A` - Any type implementing `ActiveModelTrait` and `ActiveModelBehavior`
    ///
    /// # Arguments
    ///
    /// * `active_model` - The ActiveModel instance to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful insertion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - Constraint violation occurs (unique, foreign key, etc.)
    /// - Data type mismatch
    async fn insert<A>(&self, active_model: A) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>;

    /// Inserts multiple records in a single query.
    ///
    /// Generic bulk insert function optimized for performance by
    /// batching all records into a single INSERT statement.
    ///
    /// # Type Parameters
    ///
    /// * `A` - Any type implementing `ActiveModelTrait` and `ActiveModelBehavior`
    ///
    /// # Arguments
    ///
    /// * `active_models` - Vector of ActiveModel instances to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful insertion. Returns immediately
    /// if the input vector is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - Any record violates constraints
    /// - Partial failure may occur (no automatic rollback)
    ///
    /// # Note
    ///
    /// For atomic operations where all records must succeed or fail together,
    /// use [`insert_many_with_transaction`] instead.
    async fn insert_many<A>(&self, active_models: Vec<A>) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>;

    /// Inserts multiple records within a database transaction.
    ///
    /// Transactional bulk insert with all-or-nothing semantics:
    /// if any record fails to insert, the entire operation is rolled back.
    ///
    /// # Type Parameters
    ///
    /// * `A` - Any type implementing `ActiveModelTrait` and `ActiveModelBehavior`
    ///
    /// # Arguments
    ///
    /// * `active_models` - Vector of ActiveModel instances to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful insertion of all records.
    /// Returns immediately if the input vector is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Transaction cannot be started
    /// - Any record fails to insert (triggers rollback)
    /// - Transaction commit fails
    ///
    /// # Transaction Behavior
    ///
    /// ```text
    /// BEGIN TRANSACTION
    ///     │
    ///     ├──► INSERT all records
    ///     │         │
    ///     │    Success? ──► COMMIT ──► Ok(())
    ///     │         │
    ///     │    Failure? ──► ROLLBACK ──► Err(...)
    ///     │
    /// END
    /// ```
    async fn insert_many_with_transaction<A>(&self, active_models: Vec<A>) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>;

    /// Returns a reference to the underlying database connection.
    ///
    /// Provides direct access to the SeaORM `DatabaseConnection` for
    /// custom queries not covered by the repository methods.
    ///
    /// # Returns
    ///
    /// A reference to the `DatabaseConnection` instance.
    ///
    /// # Use Cases
    ///
    /// - Complex queries with joins
    /// - Raw SQL execution
    /// - Custom transaction management
    fn get_connection(&self) -> &DatabaseConnection;
}

/// Concrete implementation of the MySQL repository.
///
/// `MysqlRepositoryImpl` manages the database connection and provides
/// generic methods for common CRUD operations using SeaORM.
///
/// # Connection Management
///
/// The connection is established once during initialization and reused
/// for all subsequent operations. SeaORM handles connection pooling
/// internally.
///
/// # Thread Safety
///
/// The `DatabaseConnection` is thread-safe and can be shared across
/// multiple async tasks without additional synchronization.
///
/// # Examples
///
/// ```rust,no_run
/// use crate::repository::MysqlRepositoryImpl;
/// use crate::entity::users;
///
/// let mysql_repo = MysqlRepositoryImpl::new().await?;
///
/// // Insert a single record
/// let user = users::ActiveModel {
///     name: Set("John".to_string()),
///     ..Default::default()
/// };
/// mysql_repo.insert(user).await?;
///
/// // Bulk insert with transaction
/// let users = vec![user1, user2, user3];
/// mysql_repo.insert_many_with_transaction(users).await?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct MysqlRepositoryImpl {
    /// The SeaORM database connection instance.
    ///
    /// This connection is thread-safe and managed by SeaORM's
    /// internal connection pool.
    db_conn: DatabaseConnection,
}

impl MysqlRepositoryImpl {
    /// Creates a new `MysqlRepositoryImpl` instance.
    ///
    /// Establishes a connection to the MySQL database using the
    /// connection string from environment variables.
    ///
    /// # Environment Variables
    ///
    /// * `DATABASE_URL` - Required. MySQL connection string
    ///   (e.g., `mysql://user:password@host:port/database`)
    ///
    /// # Returns
    ///
    /// Returns `Ok(MysqlRepositoryImpl)` on successful connection.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - `DATABASE_URL` environment variable is not set
    /// - Database connection cannot be established
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use crate::repository::MysqlRepositoryImpl;
    ///
    /// // Ensure DATABASE_URL is set:
    /// // DATABASE_URL=mysql://root:password@localhost:3306/mydb
    ///
    /// let mysql_repo = MysqlRepositoryImpl::new().await?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub async fn new() -> anyhow::Result<Self> {
        // Load database URL from environment
        //let db_url: String = ENV.mysql().database_url().to_string();
        let app_config: &AppConfig = AppConfig::global();
        let db_url: &String = app_config.es_db_url();

        // Establish database connection
        let db_conn: DatabaseConnection = Database::connect(db_url)
            .await
            .expect("[MysqlRepositoryImpl::new] Database connection failed");

        Ok(Self { db_conn })
    }
}

#[async_trait]
impl MysqlRepository for MysqlRepositoryImpl {
    /// Inserts a single record into the database.
    ///
    /// Uses SeaORM's `insert` method on the ActiveModel to persist
    /// the record to the corresponding table.
    ///
    /// # Arguments
    ///
    /// * `active_model` - The ActiveModel instance containing data to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful insertion.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert operation fails.
    async fn insert<A>(&self, active_model: A) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>,
    {
        // Execute single record insert
        active_model
            .insert(&self.db_conn)
            .await
            .map_err(|e| anyhow!("[MysqlRepositoryImpl::insert] Failed to insert: {:?}", e))?;

        Ok(())
    }

    /// Inserts multiple records in a single query.
    ///
    /// Optimizes performance by batching all records into one INSERT statement
    /// rather than executing individual inserts.
    ///
    /// # Arguments
    ///
    /// * `active_models` - Vector of ActiveModel instances to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful insertion.
    ///
    /// # Note
    ///
    /// This method does NOT use transactions. For atomic operations,
    /// use `insert_many_with_transaction` instead.
    async fn insert_many<A>(&self, active_models: Vec<A>) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>,
    {
        // Early return for empty input
        if active_models.is_empty() {
            return Ok(());
        }

        // Execute bulk insert in single query
        A::Entity::insert_many(active_models)
            .exec(&self.db_conn)
            .await
            .map_err(|e| {
                anyhow!(
                    "[MysqlRepositoryImpl::insert_many] Failed to bulk insert: {:?}",
                    e
                )
            })?;

        Ok(())
    }

    /// Inserts multiple records within a database transaction.
    ///
    /// Provides atomic bulk insert where either all records are inserted
    /// successfully, or none are (rollback on any failure).
    ///
    /// # Arguments
    ///
    /// * `active_models` - Vector of ActiveModel instances to insert
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when all records are successfully committed.
    ///
    /// # Transaction Flow
    ///
    /// 1. Begin transaction
    /// 2. Execute bulk insert (single query for performance)
    /// 3. Commit on success / Rollback on failure
    async fn insert_many_with_transaction<A>(&self, active_models: Vec<A>) -> anyhow::Result<()>
    where
        A: ActiveModelTrait + ActiveModelBehavior + Send + 'static,
        <A::Entity as EntityTrait>::Model: Sync + IntoActiveModel<A>,
    {
        // Early return for empty input
        if active_models.is_empty() {
            return Ok(());
        }

        // Begin database transaction
        let txn: DatabaseTransaction = self
            .db_conn
            .begin()
            .await
            .map_err(|e| {
                anyhow!(
                    "[MysqlRepositoryImpl::insert_many_with_transaction] Failed to begin transaction: {:?}",
                    e
                )
            })?;

        // Execute bulk insert within transaction (single query for performance)
        A::Entity::insert_many(active_models)
            .exec(&txn)
            .await
            .map_err(|e| {
                anyhow!(
                    "[MysqlRepositoryImpl::insert_many_with_transaction] Failed to bulk insert: {:?}",
                    e
                )
            })?;

        // Commit transaction on success
        txn.commit().await.map_err(|e| {
            anyhow!(
                "[MysqlRepositoryImpl::insert_many_with_transaction] Failed to commit transaction: {:?}",
                e
            )
        })?;

        Ok(())
    }

    /// Returns a reference to the underlying database connection.
    ///
    /// # Returns
    ///
    /// A reference to the `DatabaseConnection` for custom query execution.
    fn get_connection(&self) -> &DatabaseConnection {
        &self.db_conn
    }
}
