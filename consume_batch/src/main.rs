/*
Author      : Seunghwan Shin
Create date : 2025-01-01
Description :

History     : 2025-01-01 Seunghwan Shin       # [v.1.0.0] first create
              2025-03-22 Seunghwan Shin       # [v.1.1.0] Change the RDB-related crate (Diesel -> Sear-orm)
              2025-05-28 Seunghwan Shin       # [v.1.1.1] Correct duplicate index problem
              2025-06-09 Seunghwan Shin       # [v.1.1.2] Unindexable issues exist when duplicate documents exist
              2026-02-00 Seunghwan Shin       # [v.2.0.0]
*/

mod common;
use common::*;

mod utils_module;

mod controller;
use controller::batch_controller::*;

mod entity;

mod service;
use service::{
    batch_service_impl::*, consume_service_impl::*, elastic_service_impl::*, mysql_service_impl::*,
    producer_service_impl::*,
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

// Type aliases asd
type ElasticService = ElasticServiceImpl<EsRepositoryImpl>;
type MysqlService = MysqlServiceImpl<MysqlRepositoryImpl>;
type ConsumeService = ConsumeServiceImpl<KafkaRepositoryImpl>;
type ProducerService = ProducerServiceImpl<KafkaRepositoryImpl>;
type BatchService = BatchServiceImpl<MysqlService, ElasticService, ConsumeService, ProducerService>;
type Controller = BatchController<BatchService>;

#[tokio::main]
async fn main() {
    dotenv().ok();
    set_global_logger();

    info!("Indexing Batch Program Start.");

    AppConfig::init().expect("Failed to initialize AppConfig");

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
    let batch_service: BatchService = match BatchServiceImpl::new(
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

    // Create controller with batch service
    let batch_controller: Controller = BatchController::new(batch_service);

    match batch_controller.main_task().await {
        Ok(_) => (),
        Err(e) => {
            error!("{:#}", e);
        }
    }
}
