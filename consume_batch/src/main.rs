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

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

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
    AppConfig::init().expect("Failed to initialize AppConfig");
    set_global_logger();

    //run_cli_mode().await;

    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(|s| s == "--cli").unwrap_or(false) {
        run_cli_mode().await;
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

/// CLI Mode: Interactive client for executing batches on a running service
///
/// Workflow:
/// 1. Connect to the running service via Unix Socket
/// 2. Display menu and read user input
/// 3. Send command to server and display response
/// 4. Repeat until user exits with "0" or "q"
async fn run_cli_mode() {
    info!("Indexing Batch Program Start. [CLI mode]");

    // Connect to server
    let socket_path: &str = AppConfig::global().socket_path();
    let stream: UnixStream = match UnixStream::connect(socket_path).await {
        Ok(stream) => stream,
        Err(_) => {
            eprintln!(
                "[ERROR] Unable to connect to the service. ({})",
                socket_path
            );
            eprintln!("The service must be running before connecting.");
            return;
        }
    };

    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader: tokio::io::BufReader<tokio::io::ReadHalf<UnixStream>> =
        tokio::io::BufReader::new(read_half);
    let mut writer: tokio::io::WriteHalf<UnixStream> = write_half;

    loop {
        // Read and display server messages (menu, prompts, results)
        if let Err(e) = read_until_prompt(&mut reader).await {
            eprintln!("[ERROR] Failed to read from server: {}", e);
            break;
        }

        // Read user input
        let user_input: String = match read_user_input().await {
            Some(input) => input,
            None => break, // EOF or error
        };

        // Send user input to server
        if let Err(e) = writer.write_all(user_input.as_bytes()).await {
            eprintln!("[ERROR] Failed to send to server: {}", e);
            break;
        }

        // Exit if user entered quit command
        if is_exit_command(&user_input) {
            // Read final "Exiting" message from server
            let _ = read_until_prompt(&mut reader).await;
            break;
        }
    }
}

/// Reads and prints server messages until we encounter a prompt (ends with "Input: ")
async fn read_until_prompt(
    reader: &mut tokio::io::BufReader<tokio::io::ReadHalf<UnixStream>>,
) -> std::io::Result<()> {
    let mut buffer: String = String::new();

    loop {
        buffer.clear();
        let bytes_read: usize = reader.read_line(&mut buffer).await?;

        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Server closed connection",
            ));
        }

        print!("{}", buffer);

        if let Err(e) = std::io::stdout().flush() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to flush stdout: {}", e),
            ));
        }

        // Stop when we encounter the input prompt
        if buffer.trim_end().ends_with("Input:") {
            break;
        }
    }

    Ok(())
}

/// Reads a line of input from the user (blocking operation in separate thread)
async fn read_user_input() -> Option<String> {
    tokio::task::spawn_blocking(|| {
        let mut line: String = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line),
        }
    })
    .await
    .ok()
    .flatten()
}

/// Checks if the input is an exit command
fn is_exit_command(input: &str) -> bool {
    let trimmed: &str = input.trim();
    trimmed == "0" || trimmed.eq_ignore_ascii_case("q")
}
