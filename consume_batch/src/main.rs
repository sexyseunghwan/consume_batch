
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
use service::query_service::*;

#[tokio::main]
async fn main() {
    
    let set_global_logger = set_global_logger();
    dotenv().ok();

    let query_service = QueryServicePub::new();
    let main_controller = MainController::new(query_service);    

    main_controller.migration_elastic_to_rdb().await.unwrap();
    
}
