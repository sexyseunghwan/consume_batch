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

use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Type aliases asd
type ElasticService = ElasticServiceImpl<EsRepositoryImpl>;
type MysqlService = MysqlServiceImpl<MysqlRepositoryImpl>;
type ConsumeService = ConsumeServiceImpl<KafkaRepositoryImpl>;
type ProducerService = ProducerServiceImpl<KafkaRepositoryImpl>;
type BatchService = BatchServiceImpl<MysqlService, ElasticService, ConsumeService, ProducerService>;
type Controller = BatchController<BatchService>;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s == "--cli").unwrap_or(false) {
        dotenv().ok();
        AppConfig::init().expect("Failed to initialize AppConfig");
        run_cli_mode().await;
        return;
    }

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

/// `--cli` mode: 실행 중인 서비스에 Unix Socket으로 연결하고
/// 서버에서 보내는 메뉴를 그대로 출력하며 사용자 입력을 전달합니다.
async fn run_cli_mode() {
    let socket_path: String = AppConfig::global().socket_path().clone();

    let stream: UnixStream = match UnixStream::connect(&socket_path).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!(
                "[ERROR] Unable to connect to the service. ({})",
                socket_path
            );
            eprintln!("The service must be running before connecting.");
            return;
        }
    };

    let (mut sock_reader, mut sock_writer) = stream.into_split();

    // Print text recrived from the socket to stdout (display prompts immediately, even without a newline).
    let read_task = tokio::spawn(async move {
        let mut buf: Vec<u8> = vec![0u8; 4096];

        loop {
            match sock_reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    use std::io::Write;
                    print!("{}", String::from_utf8_lossy(&buf[..n]));
                    std::io::stdout().flush().unwrap();
                }
            }
        }
    });

    // Read input from stdin and forward it to the socket.
    loop {
        let input: Option<String> = tokio::task::spawn_blocking(|| {
            let mut s: String = String::new();
            match std::io::stdin().read_line(&mut s) {
                Ok(0) | Err(_) => None,
                Ok(_) => Some(s),
            }
        })
        .await
        .unwrap();

        match input {
            None => break,
            Some(line) => {
                let trimmed = line.trim();
                let should_exit = trimmed == "0" || trimmed.eq_ignore_ascii_case("q");

                if sock_writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }

                if should_exit {
                    // 서버가 "종료합니다." 메시지를 출력할 시간을 줌
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    break;
                }
            }
        }
    }

    read_task.abort();
}
