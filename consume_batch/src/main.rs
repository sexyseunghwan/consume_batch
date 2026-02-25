/*
Author      : Seunghwan Shin
Create date : 2025-01-01
Description :

History     : 2025-01-01 Seunghwan Shin       # [v.1.0.0] first create
              2025-03-22 Seunghwan Shin       # [v.1.1.0] Change the RDB-related crate (Diesel -> Sear-orm)
              2025-05-28 Seunghwan Shin       # [v.1.1.1] Correct duplicate index problem
              2025-06-09 Seunghwan Shin       # [v.1.1.2] Unindexable issues exist when duplicate documents exist
              2026-00-00 Seunghwan Shin       # [v.2.0.0]
*/

mod common;
use common::*;

mod utils_module;

mod controller;
use controller::cli_client_controller::CliClientController;
use controller::main_controller::*;

mod entity;

mod service;
use service::{
    batch_service_impl::*, cli_service_impl::CliServiceImpl, consume_service_impl::*,
    elastic_service_impl::*, mysql_service_impl::*, producer_service_impl::*,
};

mod service_trait;

mod repository;
use repository::{es_repository::*, kafka_repository::*, mysql_repository::*};

mod models;

mod app_config;
use app_config::AppConfig;

mod global_state;
use global_state::*;

mod config;

mod enums;

// Type aliases
type ElasticService = ElasticServiceImpl<EsRepositoryImpl>;
type MysqlService = MysqlServiceImpl<MysqlRepositoryImpl>;
type ConsumeService = ConsumeServiceImpl<KafkaRepositoryImpl>;
type ProducerService = ProducerServiceImpl<KafkaRepositoryImpl>;
type BatchSvc = BatchServiceImpl<MysqlService, ElasticService, ConsumeService, ProducerService>;
type CliSvc = CliServiceImpl<BatchSvc>;
type Controller = MainController<BatchSvc, CliSvc>;

/// Application entry point.
///
/// Initializes global configuration, logger, and all service dependencies,
/// then routes execution to either service mode or CLI client mode based on
/// the command-line arguments passed at startup.
///
/// # Execution Modes
///
/// ```text
/// ./consume_batch_v1           → Service mode (cron scheduler + CLI socket server)
/// ./consume_batch_v1 --cli     → CLI client mode (connects to a running service)
/// ```
///
/// # Service Mode Initialization Order
///
/// ```text
/// 1. Load environment variables (.env)
/// 2. Initialize AppConfig and global logger
/// 3. Create repositories: Elasticsearch, MySQL, Kafka
/// 4. Build services via dependency injection:
///      ElasticService / MysqlService / ConsumeService / ProducerService
///      → BatchService (wraps all sub-services)
///      → CliService   (holds Arc<BatchService> for on-demand execution)
/// 5. Assemble MainController<BatchSvc, CliSvc>
/// 6. Start batch scheduler + CLI socket server
/// ```
///
/// # Panics
///
/// Panics if any repository or service fails to initialize, since the application
/// cannot run without valid connections to all external systems (ES, MySQL, Kafka).
#[tokio::main]
async fn main() {
    dotenv().ok();
    AppConfig::init().expect("Failed to initialize AppConfig");
    set_global_logger();

    let args: Vec<String> = std::env::args().collect();

    // Option to run the application in CLI mode.
    if args.get(1).map(|s| s == "--cli").unwrap_or(false) {
        CliClientController::run().await;
        return;
    }

    info!("Indexing Batch Program Start.");

    let elastic_repo: EsRepositoryImpl = match EsRepositoryImpl::new() {
        Ok(es_repo) => es_repo,
        Err(e) => {
            error!("[main] elastic_repo: {:#}", e);
            panic!("[main] elastic_repo: {:#}", e);
        }
    };

    let mysql_repo: MysqlRepositoryImpl = match MysqlRepositoryImpl::new().await {
        Ok(sql_repo) => sql_repo,
        Err(e) => {
            error!("[main] mysql_repo: {:#}", e);
            panic!("[main] mysql_repo: {:#}", e);
        }
    };

    let kafka_repo: KafkaRepositoryImpl = match KafkaRepositoryImpl::new() {
        Ok(kafka_repo) => kafka_repo,
        Err(e) => {
            error!("[main] kafka_repo: {:#}", e);
            panic!("[main] kafka_repo: {:#}", e);
        }
    };

    let shared_kafka_repo: Arc<KafkaRepositoryImpl> = Arc::new(kafka_repo);

    // Initialize services with dependency injection
    let elastic_query_service: ElasticService = ElasticServiceImpl::new(elastic_repo);
    let mysql_query_service: MysqlService = MysqlServiceImpl::new(mysql_repo);

    // Share Kafka repository across multiple services (clone is cheap - only Arc increment)
    let consume_service: ConsumeService = ConsumeServiceImpl::new(Arc::clone(&shared_kafka_repo));
    let producer_service: ProducerService =
        ProducerServiceImpl::new(Arc::clone(&shared_kafka_repo));

    // Create batch service with all dependencies
    let batch_service: BatchSvc = match BatchServiceImpl::new(
        mysql_query_service,
        elastic_query_service,
        consume_service,
        producer_service,
    ) {
        Ok(batch_service) => batch_service,
        Err(e) => {
            error!("[main] batch_service: {:#}", e);
            panic!("[main] batch_service: {:#}", e);
        }
    };
    
    // Create CLI service: shares the batch service via Arc for on-demand execution
    let cli_service: CliSvc = CliServiceImpl::new(
        Arc::new(batch_service.clone()),
        batch_service.schedule_config().clone(),
    );

    // Create main controller with both services
    let main_controller: Controller = MainController::new(batch_service, Arc::new(cli_service));

    match main_controller.main_task().await {
        Ok(_) => (),
        Err(e) => {
            error!("[main] {:#}", e);
        }
    }
}
