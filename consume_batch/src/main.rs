mod common;
use common::*;

mod utils_module;
use utils_module::logger_utils::*;

mod repository;

mod models;

mod schema;

mod controller;
use controller::main_controller::*;

mod service;
use service::es_query_service::*;
use service::query_service::*;

#[tokio::main]
async fn main() {
    let set_global_logger = set_global_logger();
    dotenv().ok();

    let query_service = QueryServicePub::new();
    let es_query_service = EsQueryServicePub::new();
    let main_controller = MainController::new(query_service, es_query_service);

    main_controller.insert_es_to_mysql().await.unwrap();
}
